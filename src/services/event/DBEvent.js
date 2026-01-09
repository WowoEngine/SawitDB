/**
 * @typedef {Object} DBEvent
 * @property {() => void} onTableSelected(table, data, aql)
 * @property {() => void} onTableUpdated(table, data, aql)
 * @property {() => void} onTableDeleted(table, data, aql)
 * @property {() => void} onTableInserted(table, data, aql)
 * @property {() => void} OnTableCreated(table, data, aql)
 * @property {() => void} OnTableDropped(table, data, aql)
 */

class DBEvent {
  /**
   * @param {Object} options
   */
  constructor(options) {
    if (new.target === DBEvent) {
      throw new Error("DBEvent is abstract and cannot be instantiated");
    }    
    this.logFile = process.env.SAWIT_CDC_FILE;
    this.adapter = process.env.SAWIT_CDC_ADAPTER;
  }  
  /**
   * @param {string} table
   * @param {array} data
   * @param {string} aql
   */
  OnTableSelected(table, data, aql) {  }
  
  /**
   * @param {string} table
   * @param {array} data
   * @param {string} aql
   */
  OnTableUpdated(table, data, aql) {  }
  /**
   * @param {string} table
   * @param {array} data
   * @param {string} aql
   */
  OnTableDeleted(table, data, aql) {  }
  /**
   * @param {string} table
   * @param {array} data
   * @param {string} aql
   */
  OnTableInserted(table, data, aql) {  }
  /**
   * @param {string} table
   * @param {Object} tableObj
   * @param {string} aql
   */
  OnTableCreated(table, tableObj, aql) {  }
  /**
   * @param {string} table
   * @param {Object} tableObj
   * @param {string} aql
   */
  OnTableDropped(table, tableObj, aql) {  }
}

module.exports = DBEvent;
