# Informe — TP Coordinación

## Solución

El sistema implementa un pipeline de procesamiento distribuido que calcula el top N de frutas por cantidad total, procesando de forma concurrente las consultas de múltiples clientes simultáneos.

El flujo de datos es el siguiente:


Cliente → Gateway → Sum (N instancias) → Aggregator (M instancias) → Join → Gateway → Cliente


## Decisiones de diseño

### Separación de flujos por client_id

El Gateway asigna un client_id a cada cliente. Ese id viaja en todos los mensajes internos del sistema para que los diferentes Sums, Aggregators, el Join y devuelta el Gateway sepan de que cliente es cada dato.

### Colas compartidas para datos, exchanges para control

Los mensajes de datos (pares fruta/cantidad) se envían a colas compartidas entre los diferentes Sums (las entidades con multiples instancias). RabbitMQ distribuye esos mensajes con prefetch_count = 1 entre los consumidores, logrando balanceo de carga y haciendo que los workers mas rapidos consuman mas mensajes mientras que los mas lentos consuman menos ya que cada worker puede tener asignado solo un mensaje a la vez.

Los mensajes de control (los EOF) se envían a través de exchanges de tipo direct con una unica routing key igual al nombre del exchange. Cada instancia se suscribe al mismo exchange con su propia cola. Esto es porque del Gateway sale solo un EOF de cada cliente y éste es consumido solo por un Sum, por lo que lo debe propagar al resto de sus compañeros por medio de este exchange. De esta manera todos reciben el EOF y saben que no van a llegar mas datos de ese cliente.

Como pika no es thread-safe, cada cola o exchange se consume en un thread diferente con su propia conexión y canal independientes. En el Sum el hilo principal consume la cola de datos y el thread auxiliar consume el exchange de control, cada uno con su propia instancia de conexión. Mas adelante en el informe justificamos el uso de threads en vez de procesos.

Todo esto es análogo en los Aggregators.

### Protocolo de fin de procesamiento de un cliente en Sum

Cuando un Sum recibe el EOF de un cliente (mensaje de un solo campo [client_id]), hace tres cosas:

1. Envía sus totales acumulados al Aggregator a través de la cola de datos compartida.
2. Envía el EOF de ese cliente a la misma cola.
3. Publica un mensaje de control en el exchange "SUM_CONTROL_EXCHANGE" con su propio id.

Cada instancia de Sum escucha ese mismo exchange en un thread separado. Al recibir un EOF, verifica si el sender_id coincide con su propio id, si es así, lo ignora (es su propio mensaje rebotado), si no, sabe que otra instancia procesó el EOF y envía también sus propios totales para ese cliente. Así, todas las instancias de Sum vacían su estado acumulado para ese client_id, independientemente de cuál recibió el EOF original.

### Caso borde

Cuando un Sum recibe un EOF de otro Sum y no de la cola compartida hay tres casos que hay que contemplar:

1. Le llego el EOF mientras está procesando el último dato del cliente.
2. Le llego el EOF y el último dato del cliente está en vuelo, es decir, salió de la cola compartida antes del EOF pero todavía no llego al Sum.
3. Le llego el EOF y ya procesó todos los datos del cliente por lo que debe mandar los datos y el EOF al Aggregation.

Para el caso 1 la solucion fue agregar un threading.Event (flag de sincronización entre threads) llamado data_idle que lo que hace es avisarle al thread del exchange si en ese momento se esta esperando el siguiente mensaje o si esta procesando alguno en ese momento. Si esta procesando un mensaje espera hasta que termine antes de seguir. Si está esperando un mensaje vamos a los siguientes casos.

Para el caso 2 y 3, si no estoy procesando ningun dato en ese momento, en realidad no puedo diferenciar en cual de los dos casos estoy. Puede ser que hay un dato que todavía no llego o puede ser que no van a llegar mas datos de ese cliente y tengo que mandar los datos y el EOF al Aggregator. Por esa razon se agregó un segundo threading.Event llamado data_arrived que se activa cuando el hilo principal empieza a procesar un dato y se limpia cuando termina. El thread del exchange espera hasta 1 segundo a que data_arrived se active. Si durante ese segundo llega un dato, espera a que termine de procesarse y recién entonces procesa el EOF. Si no llega ningún dato en ese segundo, la probabilidad de que el dato en vuelo esté pero todavia no haya llegado es muy baja por lo que asumimos ese riesgo y procesa el EOF directamente. Pero lo mas probable es que no hayan mas datos de ese cliente.

### Protocolo de fin de ingesta en Aggregator

Cada instancia de Aggregator mantiene un contador eof_count_by_client. Cada vez que recibe un EOF (ya sea en el hilo de datos o en el hilo de control), incrementa ese contador. Cuando el contador alcanza SUM_AMOUNT (la cantidad total de instancias de Sum), calcula el top parcial para ese cliente y lo envía al Join. Esto garantiza que el Aggregator solo produce resultados una vez que recibió los datos de todos los Sum de ese cliente.

El mecanismo es análogo al de Sum: cada instancia publica en el exchange de control de Aggregator y las demás instancias reciben el broadcast, acumulando tambien su contador.

El Aggregator tiene los mismos casos borde que Sum con los datos en procesamiento o en vuelo, y los resuelve de la misma manera: los threading.Events data_idle y data_arrived con el timeout de 1 segundo.

### Join

El Join recibe tops parciales de todas las instancias de Aggregator. Mantiene un contador eof_count_by_client y acumula los resultados hasta recibir AGGREGATION_AMOUNT tops para un mismo cliente. En ese punto mergea los resultados, calcula el top final y lo envía al Gateway para que se lo entregue al cliente correspondiente.

### Manejo de SIGTERM

Todos los controles registran un handler de SIGTERM que invoca stop_consuming() en todas sus colas/exchanges activos. Esto hace que start_consuming() retorne, y el bloque finally cierra todas las conexiones con RabbitMQ limpiamente. El Gateway ademas cierra todos los sockets de clientes activos antes de terminar.


## Escalabilidad

### Respecto a los clientes

El Gateway acepta múltiples conexiones concurrentes. Por cada cliente que se conecta lanza un proceso del pool para manejar su ingesta. El proceso de respuesta (handle_client_response) es único y rutea cada resultado al socket del cliente correcto usando el client_id.

### Respecto a la cantidad de controles

Aumentar la cantidad de instancias de Sum o Aggregator no requiere cambios en el código: solo se modifican las variables de entorno SUM_AMOUNT y AGGREGATION_AMOUNT en el docker-compose. Cada control lee estas variables y las usa para saber cuántos EOFs tiene que esperar antes de mandar sus resultados. Las colas compartidas de datos permiten que RabbitMQ balancee el trabajo automáticamente entre las nuevas instancias.

## Uso de threads en lugar de procesos

Sum y Aggregator usan un thread adicional para consumir el exchange de control, mientras que el hilo principal consume la cola de datos. La razón principal para usar threads en lugar de procesos es que el GIL de Python nos afecta muy poco en este caso: las operaciones son mayormente de I/O (leer de RabbitMQ, enviar a colas), que liberan el GIL, y el procesamiento requerido (sumas, inserciones en lista ordenada) es lo suficientemente liviano como para que dos threads compitan muy poco por él. Usar procesos no haría el programa mucho más eficiente, y sí agregaría complejidad innecesaria:

1. **Estado compartido sin IPC**: el hilo de datos y el hilo de control necesitan acceder al mismo diccionario de acumulación (amount_by_fruit_by_client en Sum, fruit_top_by_client en Aggregator). Con threads esto se logra con un threading.Lock. Con procesos habría que usar multiprocessing.Manager o colas IPC, lo que complica bastante el código sin ganar nada real.

2. **Sincronización más simple**: el thread de control necesita esperar a que el hilo principal no esté procesando datos antes de enviar el estado del cliente. Con threads esto se resuelve con threading.Events (data_idle, data_arrived). Con procesos, implementar esa misma coordinación seria mas complicado.

## Otros casos borde identificados

### Cliente desconectado antes de recibir el resultado

Si el socket de un cliente se rompe antes de que el Gateway pueda entregarle el resultado, el Gateway captura el socket.error, elimina al cliente de la lista y hace ack del mensaje de RabbitMQ para no bloquearse. El resultado se descarta.

### SIGTERM con clientes activos

El handler de SIGTERM en el Gateway cierra los sockets de todos los clientes activos antes de terminar. Esto hace que los procesos hijos que están leyendo de esos sockets reciban un socket.error y terminen limpiamente cerrando su cola de salida en el bloque finally.
