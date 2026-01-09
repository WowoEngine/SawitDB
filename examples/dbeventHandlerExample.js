const DBEvent = require("../src/services/event/DBEvent");
const fs = require("fs");
const path = require("path");
/**
 * @implements {DBEvent}
 */
class DBEventHandlerExample extends DBEvent {
    
  constructor(){
    super();
  }
  writeCDC(aql) {
    const log = `${aql}\n`;
    fs.appendFile(this.logFile, log, (err) => {
        if (err) console.error("Failed to write cdc:", err);
    });
 }
  OnTableSelected(table, data, aql) {
    console.log("onTableSelected "+table,data);
    console.log("AQL "+table,aql);
    this.writeCDC(aql);
  }
  OnTableUpdated(table, data, aql) {
    console.log("onTableUpdated "+table,data);
    console.log("AQL "+table,aql);
    this.writeCDC(aql);
  }
  
  OnTableDeleted(table, data, aql) { 
    console.log("OnTableDeleted "+table,data);
    console.log("AQL "+table,aql);
    this.writeCDC(aql);
 }
  OnTableInserted(table, data, aql) {     
    console.log("OnTableInserted "+table,data);
    console.log("AQL "+table,aql);
    this.writeCDC(aql);
  }

  OnTableCreated(table, data, aql) {     
    console.log("OnTableCreated "+table,data);
    console.log("AQL "+table,aql);
   }
  OnTableDropped(table, data, aql) {     
    console.log("OnTableDropped "+table,data);
    console.log("AQL "+table,aql);
    this.writeCDC(aql);
   }
}

module.exports = DBEventHandlerExample;
