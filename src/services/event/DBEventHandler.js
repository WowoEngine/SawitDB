const DBEvent = require("./DBEvent");

/**
 * @implements {DBEvent}
 */
class DBEventHandler extends DBEvent {
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
    if(this.adapter=="cpo") this.writeCDC(aql);
  }
  OnTableUpdated(table, data, aql) {
   if(this.adapter=="cpo")  this.writeCDC(aql);
  }
  
  OnTableDeleted(table, data, aql) { 
    if(this.adapter=="cpo") this.writeCDC(aql);
 }
  OnTableInserted(table, data, aql) {  
    if(this.adapter=="cpo") this.writeCDC(aql);
    
  }

  OnTableCreated(table, data, aql) {  
    if(this.adapter=="cpo") this.writeCDC(aql);  
   }
  OnTableDropped(table, data, aql) {  
    if(this.adapter=="cpo") this.writeCDC(aql);  
   }
}

module.exports = DBEventHandler;
