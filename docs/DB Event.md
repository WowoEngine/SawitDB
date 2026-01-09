
**DB Event** is a feature for  `sawitDB` to provide Event Drivent capabilities especially regarding Change Data Capture. This feature is listening for event below:
| Event Name | Description|
|---------|-------------------|
| OnTableCreated | Triggered when new table is crated |
| OnTableDropped | Triggered when a table is dropped |
| OnTableSelected | Triggered when select record from a table |
| OnTableInserted | Triggered when insert record into a table |
| OnTableUpdated | Triggered when a table updataed rows |
| OnTableDeleted | Triggered when rows deleted from a table |

## Enabled CDC (Change Data Capture)
SawitDB is supported for CDC, to enabled CDC you need follow these step
1. Determined **CDC Adapter**, currently we still only support **CPO** adapter. 
2. **CDC Adapter** captures change on SawitDB and write as AQL to file `.cpo`, all changed will be recorded on this file. Below how to config CDC and Adapter on `.env` for server side

```
SAWIT_CDC_FILE=./examples/logs/sawit.cpo
SAWIT_CDC_ADAPTER=cpo
```
Next will be supported for Kafka
## Disabled CDC
If you don't want use CDC you just need let `SAWIT_CDC_ADAPTER` empty on .env
## Custome Event Handler
By default EventHandler provide by `./src/services/event/DBEventHandler.js` but you can use custom event handler as below:
```
const event = require('./dbeventHandlerExample')
const db = new SawitDB(dbPath,{dbevent:new event()});
```
Full example can view on  `./examples/dbeventHandlerExample.js` and for using it `./examples/dbevent.js`.
To see output CDC on `./examples/logs/sawit.cpo` 

