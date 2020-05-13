const DAO = require('./dao')

class SQLiteWriter {
    /**
     * Writer for SQLite databases
     * @param {DAO} dao 
     */
    constructor (dao) {
        this.dao = dao
    }

    /**
     * Turns on WAL / Write Ahead Logging
     */
    setWalMode () {
        return this.dao.run('PRAGMA journal_mode = WAL;')
    }

    /**
     * Serialize queries
     * @param {*} callback What should be serialized
     */
    serialize (callback) {
        return this.dao.serialize(callback)
    }

    /**
     * Parallelize queries
     * @param {*} callback What should be parallelized
     */
    parallelize (callback) {
        return this.dao.parallelize(callback)
    }

    /**
     * Close the database connection
     */
    closeDb () {
        return this.dao.close()
    }

    /**
     * Create a new table
     * @param {string} name Table name
     * @param {Array<string>} colNames Column names
     * @param {Array<string>} colTypes Column types
     */
    createTable (name, colNames, colTypes) {
        let sql = `CREATE TABLE IF NOT EXISTS ` + name + `(id INTEGER PRIMARY KEY AUTOINCREMENT,`

        // check if arrays match and if at least one item in them
        if (colNames.length !== colTypes.length) return false
        if (!colNames.length) return false

        for (let i = 0; i < colNames.length; i++) {
            sql += colNames[i] + ' ' + colTypes[i]
            if (i < (colNames.length - 1)) {
                sql += ', '
            }
        }
        sql += ')'
        return this.dao.run(sql)
    }

    /**
     * Drop a table
     * @param {string} name Table name
     */
    dropTable (name) {
        return this.dao.run('DROP TABLE IF EXISTS ' + name + ';')
    }

    /**
     * Insert new row into table
     * @param {string} table Table name
     * @param {Array<string>} cols Column names
     * @param {Array} data Row data
     */
    insertRow (table, cols, data) {
        let sql = 'INSERT INTO ' + table + '('

        for (let i = 0; i < cols.length; i++) {
            sql += cols[i]
            if (i < (cols.length - 1)) {
                sql += ', '
            }
        }
        sql += ') VALUES ('
        sql += data.map(() => '?').join(', ')
        sql += ')'
        return this.dao.run(sql, data)
    }

    /**
     * Insert multiple new rows into table
     * @param {string} table Table name
     * @param {Array<string>} cols Column names
     * @param {Array<Array>} data Data for each row to be inserted
     */
    insertMultipleRows (table, cols, data) {
        let sql = 'INSERT INTO ' + table + '('
        
        for (let i = 0; i < cols.length; i++) {
            sql += cols[i]
            if (i < (cols.length - 1)) {
                sql += ', '
            }
        }
        sql += ') VALUES '
        sql += data.map((value) => {
            let innerPlaceholder = value.map(() => '?').join(', ')
            return '(' + innerPlaceholder + ')'
        }).join(', ')

        // merge all sub arrays into one "toplevel" array
        let flattenedData = [].concat.apply([], data)

        return this.dao.run(sql, flattenedData)
    }

    /**
     * Update a row with new data
     * @param {string} table Table name
     * @param {Array<string>} cols Column names to update
     * @param {Array|Object} data New data to use
     * @param {string} whereCol From which column to get the row from
     * @param {*} whereValue Which value to search for at whereCol
     */
    updateRow (table, cols, data, whereCol, whereValue) {
        let sql = 'UPDATE ' + table + ' SET '

        let dataArray = []
        if (Array.isArray(data)) {
            dataArray = data
        }

        for (let i = 0; i < cols.length; i++) {
            sql += cols[i] + ' = ?'
            if (i < (cols.length - 1)) {
                sql += ', '
            }

            if (!Array.isArray(data)) {
                dataArray.push(data[cols[i]])
            }
        }
        sql += ' WHERE ' + whereCol + ' = ' + whereValue
        return this.dao.run(sql, dataArray)
    }

    /**
     * Delete a row from a table
     * @param {string} table Table name
     * @param {string} whereCol By which column to get the row
     * @param {*} whereValue Which value to search for at whereCol
     */
    deleteRow (table, whereCol, whereValue) {
        let sql = 'DELETE FROM ' + table
        sql += 'WHERE' + whereCol + ' = ' + whereValue
        return this.dao.run(sql)
    }
}

module.exports = SQLiteWriter
