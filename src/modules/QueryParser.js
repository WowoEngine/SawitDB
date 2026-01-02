
/**
 * QueryParser handles tokenizing and parsing SQL-like commands
 * Returns a Command Object: { type, table, data, criteria, ... }
 */
class QueryParser {
    constructor() { }

    tokenize(sql) {
        // Regex to match tokens
        const tokenRegex = /\s*(=>|!=|>=|<=|<>|[(),=*.<>?]|[a-zA-Z_]\w*|@\w+|\d+|'[^']*'|"[^"]*")\s*/g;
        const tokens = [];
        let match;
        while ((match = tokenRegex.exec(sql)) !== null) {
            tokens.push(match[1]);
        }
        return tokens;
    }

    parse(queryString, params) {
        const tokens = this.tokenize(queryString);
        if (tokens.length === 0) return { type: 'EMPTY' };

        const cmd = tokens[0].toUpperCase();
        let command;

        try {
            switch (cmd) {
                case 'LAHAN':
                case 'CREATE':
                    if (tokens[1] && tokens[1].toUpperCase() === 'INDEX') return this.parseCreateIndex(tokens);
                    return this.parseCreate(tokens);
                case 'LIHAT':
                case 'SHOW':
                    return this.parseShow(tokens);
                case 'TANAM':
                case 'INSERT':
                    return this.parseInsert(tokens);
                case 'PANEN':
                case 'SELECT':
                    return this.parseSelect(tokens);
                case 'GUSUR':
                case 'DELETE':
                    return this.parseDelete(tokens);
                case 'PUPUK':
                case 'UPDATE':
                    return this.parseUpdate(tokens);
                case 'BAKAR':
                case 'DROP':
                    return this.parseDrop(tokens);
                case 'INDEKS':
                    return this.parseCreateIndex(tokens);
                case 'HITUNG':
                    return this.parseAggregate(tokens);
                default:
                    throw new Error(`Perintah tidak dikenal: ${cmd}`);
            }

            if (params) {
                this._bindParameters(command, params);
            }
            return command;
        } catch (e) {
            return { type: 'ERROR', message: e.message };
        }
    }

    // --- Parser Methods ---

    parseCreate(tokens) {
        let name;
        if (tokens[0].toUpperCase() === 'CREATE') {
            if (tokens[1].toUpperCase() !== 'TABLE') throw new Error("Syntax: CREATE TABLE [name]");
            name = tokens[2];
        } else {
            if (tokens.length < 2) throw new Error("Syntax: LAHAN [nama_kebun]");
            name = tokens[1];
        }
        return { type: 'CREATE_TABLE', table: name };
    }

    parseShow(tokens) {
        const cmd = tokens[0].toUpperCase();
        const sub = tokens[1] ? tokens[1].toUpperCase() : '';

        if (cmd === 'LIHAT') {
            if (sub === 'LAHAN') return { type: 'SHOW_TABLES' };
            if (sub === 'INDEKS') return { type: 'SHOW_INDEXES', table: tokens[2] || null };
        } else if (cmd === 'SHOW') {
            if (sub === 'TABLES') return { type: 'SHOW_TABLES' };
            if (sub === 'INDEXES') return { type: 'SHOW_INDEXES', table: tokens[2] || null };
        }

        throw new Error("Syntax: LIHAT LAHAN | SHOW TABLES | LIHAT INDEKS [table] | SHOW INDEXES");
    }

    parseDrop(tokens) {
        if (tokens[0].toUpperCase() === 'DROP') {
            if (tokens[1] && tokens[1].toUpperCase() === 'TABLE') {
                return { type: 'DROP_TABLE', table: tokens[2] };
            }
        } else if (tokens[0].toUpperCase() === 'BAKAR') {
            if (tokens[1] && tokens[1].toUpperCase() === 'LAHAN') {
                return { type: 'DROP_TABLE', table: tokens[2] };
            }
        }
        throw new Error("Syntax: BAKAR LAHAN [nama] | DROP TABLE [nama]");
    }

    parseInsert(tokens) {
        let i = 1;
        let table;

        if (tokens[0].toUpperCase() === 'INSERT') {
            if (tokens[1].toUpperCase() !== 'INTO') throw new Error("Syntax: INSERT INTO [table] ...");
            i = 2;
        } else {
            if (tokens[1].toUpperCase() !== 'KE') throw new Error("Syntax: TANAM KE [kebun] ...");
            i = 2;
        }

        table = tokens[i];
        i++;

        const cols = [];
        if (tokens[i] === '(') {
            i++;
            while (tokens[i] !== ')') {
                if (tokens[i] !== ',') cols.push(tokens[i]);
                i++;
                if (i >= tokens.length) throw new Error("Unclosed parenthesis in columns");
            }
            i++;
        } else {
            throw new Error("Syntax: ... [table] (col1, ...) ...");
        }

        const valueKeyword = tokens[i].toUpperCase();
        if (valueKeyword !== 'BIBIT' && valueKeyword !== 'VALUES') throw new Error("Expected BIBIT or VALUES");
        i++;

        const vals = [];
        if (tokens[i] === '(') {
            i++;
            while (tokens[i] !== ')') {
                if (tokens[i] !== ',') {
                    let val = tokens[i];
                    if (val.startsWith("'") || val.startsWith('"')) val = val.slice(1, -1);
                    else if (val.toUpperCase() === 'NULL') val = null;
                    else if (val.toUpperCase() === 'TRUE') val = true;
                    else if (val.toUpperCase() === 'FALSE') val = false;
                    else if (!isNaN(val)) val = Number(val);
                    vals.push(val);
                }
                i++;
            }
        } else {
            throw new Error("Syntax: ... VALUES (val1, ...)");
        }

        if (cols.length !== vals.length) throw new Error("Columns and Values count mismatch");

        const data = {};
        for (let k = 0; k < cols.length; k++) {
            data[cols[k]] = vals[k];
        }

        return { type: 'INSERT', table, data };
    }

    parseSelect(tokens) {
        let i = 1;
        const cols = [];
        while (i < tokens.length && !['DARI', 'FROM'].includes(tokens[i].toUpperCase())) {
            if (tokens[i] !== ',') cols.push(tokens[i]);
            i++;
        }

        if (i >= tokens.length) throw new Error("Expected DARI or FROM");
        i++;

        const table = tokens[i];
        i++;

        let criteria = null;
        if (i < tokens.length && ['DIMANA', 'WHERE'].includes(tokens[i].toUpperCase())) {
            i++;
            // Calculate whereEndIndex by checking for ORDER or LIMIT or END
            criteria = this.parseWhere(tokens, i);
            // Move i past the WHERE clause
            // parseWhere logic assumes it stops at keywords, but we need to sync `i` in this parent method.
            // Since parseWhere doesn't return new index, we must scan forward or refactor parseWhere.
            // Simplified: scan until keyword.
            while (i < tokens.length && !['ORDER', 'LIMIT', 'OFFSET'].includes(tokens[i].toUpperCase())) {
                i++;
            }
        }

        let sort = null;
        if (i < tokens.length && tokens[i].toUpperCase() === 'ORDER') {
            i++; // ORDER
            if (tokens[i].toUpperCase() === 'BY') i++;
            const key = tokens[i];
            i++;
            let dir = 'asc';
            if (i < tokens.length && ['ASC', 'DESC'].includes(tokens[i].toUpperCase())) {
                dir = tokens[i].toLowerCase();
                i++;
            }
            sort = { key, dir };
        }

        let limit = null;
        let offset = null;

        if (i < tokens.length && tokens[i].toUpperCase() === 'LIMIT') {
            i++;
            limit = parseInt(tokens[i]);
            i++;
        }

        if (i < tokens.length && tokens[i].toUpperCase() === 'OFFSET') {
            i++;
            offset = parseInt(tokens[i]);
            i++;
        }

        return { type: 'SELECT', table, cols, criteria, sort, limit, offset };
    }

    parseWhere(tokens, startIndex) {
        const conditions = [];
        let i = startIndex;
        let currentLogic = 'AND';

        while (i < tokens.length) {
            const token = tokens[i];
            const upper = token ? token.toUpperCase() : '';

            if (upper === 'AND' || upper === 'OR') {
                currentLogic = upper;
                i++;
                continue;
            }

            if (['DENGAN', 'ORDER', 'LIMIT', 'OFFSET', 'GROUP', 'KELOMPOK'].includes(upper)) {
                break;
            }

            // Parse condition: key op val
            if (i < tokens.length - 1) {
                const key = tokens[i];
                const op = tokens[i + 1].toUpperCase();
                let val = null;
                let consumed = 2; // Default consumed for key + op

                if (op === 'BETWEEN') {
                    // Syntax: key BETWEEN v1 AND v2
                    // tokens[i] = key
                    // tokens[i+1] = BETWEEN
                    // tokens[i+2] = v1
                    // tokens[i+3] = AND
                    // tokens[i+4] = v2

                    let v1 = tokens[i + 2];
                    let v2 = tokens[i + 4];

                    // Normalize v1
                    if (v1 && (v1.startsWith("'") || v1.startsWith('"'))) v1 = v1.slice(1, -1);
                    else if (!isNaN(v1)) v1 = Number(v1);

                    // Normalize v2
                    if (v2 && (v2.startsWith("'") || v2.startsWith('"'))) v2 = v2.slice(1, -1);
                    else if (!isNaN(v2)) v2 = Number(v2);

                    conditions.push({ key, op: 'BETWEEN', val: [v1, v2], logic: currentLogic });
                    consumed = 5;

                    // Check if AND was actually present?
                    if (tokens[i + 3].toUpperCase() !== 'AND') {
                        throw new Error("Syntax: ... BETWEEN val1 AND val2");
                    }
                } else if (op === 'IS') {
                    // Syntax: key IS NULL or key IS NOT NULL
                    // tokens[i+2] could be NULL or NOT
                    const next = tokens[i + 2].toUpperCase();
                    if (next === 'NULL') {
                        conditions.push({ key, op: 'IS NULL', val: null, logic: currentLogic });
                        consumed = 3;
                    } else if (next === 'NOT') {
                        if (tokens[i + 3].toUpperCase() === 'NULL') {
                            conditions.push({ key, op: 'IS NOT NULL', val: null, logic: currentLogic });
                            consumed = 4;
                        } else {
                            throw new Error("Syntax: IS NOT NULL");
                        }
                    } else {
                        throw new Error("Syntax: IS NULL or IS NOT NULL");
                    }
                } else if (op === 'IN' || op === 'NOT') {
                    // Handle IN (...) or NOT IN (...)
                    if (op === 'NOT') {
                        if (tokens[i + 2].toUpperCase() !== 'IN') break; // invalid
                        consumed++; // skip IN
                        // Op becomes NOT IN
                    }

                    // Expect ( v1, v2 )
                    let p = (op === 'NOT') ? i + 3 : i + 2;
                    if (tokens[p] === '(') {
                        p++;
                        const values = [];
                        while (tokens[p] !== ')') {
                            if (tokens[p] !== ',') {
                                let v = tokens[p];
                                if (v.startsWith("'") || v.startsWith('"')) v = v.slice(1, -1);
                                else if (!isNaN(v)) v = Number(v);
                                values.push(v);
                            }
                            p++;
                            if (p >= tokens.length) break;
                        }
                        val = values;
                        consumed = (p - i) + 1; // +1 for closing paren
                    }
                    // Normalize OP
                    const finalOp = (op === 'NOT') ? 'NOT IN' : 'IN';
                    conditions.push({ key, op: finalOp, val, logic: currentLogic });
                    i += consumed;
                    continue;
                } else {
                    // Normal Ops (=, LIKE, etc)
                    val = tokens[i + 2];
                    if (val && (val.startsWith("'") || val.startsWith('"'))) {
                        val = val.slice(1, -1);
                    } else if (val && !isNaN(val)) {
                        val = Number(val);
                    }
                    conditions.push({ key, op, val, logic: currentLogic });
                    consumed = 3;
                }

                i += consumed;
            } else {
                break;
            }
        }

        if (conditions.length === 1) return conditions[0];
        return { type: 'compound', conditions };
    }

    parseDelete(tokens) {
        let table;
        let i;

        if (tokens[0].toUpperCase() === 'DELETE') {
            if (tokens[1].toUpperCase() !== 'FROM') throw new Error("Syntax: DELETE FROM [table] ...");
            table = tokens[2];
            i = 3;
        } else {
            if (tokens[1].toUpperCase() !== 'DARI') throw new Error("Syntax: GUSUR DARI [kebun] ...");
            table = tokens[2];
            i = 3;
        }

        let criteria = null;
        if (i < tokens.length && ['DIMANA', 'WHERE'].includes(tokens[i].toUpperCase())) {
            i++;
            criteria = this.parseWhere(tokens, i);
        }

        return { type: 'DELETE', table, criteria };
    }

    parseUpdate(tokens) {
        let table;
        let i;

        if (tokens[0].toUpperCase() === 'UPDATE') {
            table = tokens[1];
            if (tokens[2].toUpperCase() !== 'SET') throw new Error("Expected SET");
            i = 3;
        } else {
            if (tokens.length < 3) throw new Error("Syntax: PUPUK [kebun] DENGAN ...");
            table = tokens[1];
            if (tokens[2].toUpperCase() !== 'DENGAN') throw new Error("Expected DENGAN");
            i = 3;
        }

        const updates = {};
        while (i < tokens.length && !['DIMANA', 'WHERE'].includes(tokens[i].toUpperCase())) {
            if (tokens[i] === ',') { i++; continue; }
            const key = tokens[i];
            if (tokens[i + 1] !== '=') throw new Error("Syntax: key=value in update list");
            let val = tokens[i + 2];
            if (val.startsWith("'") || val.startsWith('"')) val = val.slice(1, -1);
            else if (!isNaN(val)) val = Number(val);
            updates[key] = val;
            i += 3;
        }

        let criteria = null;
        if (i < tokens.length && ['DIMANA', 'WHERE'].includes(tokens[i].toUpperCase())) {
            i++;
            criteria = this.parseWhere(tokens, i);
        }
        return { type: 'UPDATE', table, updates, criteria };
    }

    parseCreateIndex(tokens) {
        // Tani: INDEKS [table] PADA [field]
        // Generic: CREATE INDEX [name] ON [table] ( [field] )
        // OR: CREATE INDEX ON [table] ( [field] )

        if (tokens[0].toUpperCase() === 'CREATE' && tokens[1].toUpperCase() === 'INDEX') {
            let i = 2;
            // Optional Index Name (skip if present, look for ON)
            // If tokens[i] is 'ON', then no name provided. Use generic.
            // If tokens[i+1] is 'ON', then tokens[i] is name.

            if (tokens[i].toUpperCase() !== 'ON' && tokens[i + 1] && tokens[i + 1].toUpperCase() === 'ON') {
                i++; // Skip name
            }

            if (tokens[i].toUpperCase() !== 'ON') throw new Error("Syntax: CREATE INDEX ... ON [table] ...");
            i++;

            const table = tokens[i];
            i++;

            if (tokens[i] !== '(') throw new Error("Syntax: ... ON [table] ( [field] )");
            i++;

            const field = tokens[i];
            i++;

            if (tokens[i] !== ')') throw new Error("Unclosed parenthesis for index field");

            return { type: 'CREATE_INDEX', table, field };
        }

        // Tani Fallback
        if (tokens.length < 4) throw new Error("Syntax: INDEKS [table] PADA [field]");
        const table = tokens[1];
        if (tokens[2].toUpperCase() !== 'PADA') throw new Error("Expected PADA");
        const field = tokens[3];
        return { type: 'CREATE_INDEX', table, field };
    }




    parseAggregate(tokens) {
        // Syntax: HITUNG FUNC ( field ) DARI [table] ...
        // Tokens: ['HITUNG', 'SUM', '(', 'stock', ')', 'DARI', ...]
        let i = 1;

        const aggFunc = tokens[i].toUpperCase();
        i++;

        if (tokens[i] !== '(') throw new Error("Syntax: HITUNG FUNC(field) ...");
        i++;

        const aggField = tokens[i] === '*' ? null : tokens[i];
        i++;

        if (tokens[i] !== ')') throw new Error("Expected closing parenthesis");
        i++;

        if (!tokens[i] || (tokens[i].toUpperCase() !== 'DARI' && tokens[i].toUpperCase() !== 'FROM')) {
            throw new Error("Expected DARI or FROM");
        }
        i++;

        const table = tokens[i];
        i++;

        let criteria = null;
        if (i < tokens.length && ['DIMANA', 'WHERE'].includes(tokens[i].toUpperCase())) {
            i++;
            criteria = this.parseWhere(tokens, i);
            // Fast forward past WHERE clause
            while (i < tokens.length && !['KELOMPOK', 'GROUP'].includes(tokens[i].toUpperCase())) {
                i++;
            }
        }

        let groupField = null;
        if (i < tokens.length && ['KELOMPOK', 'GROUP'].includes(tokens[i].toUpperCase())) {
            // GROUP BY field
            // Syntax: GROUP BY field
            if (tokens[i].toUpperCase() === 'GROUP' && tokens[i + 1].toUpperCase() === 'BY') {
                i += 2;
            } else {
                i++; // KELOMPOK
            }
            groupField = tokens[i];
        }

        return { type: 'AGGREGATE', table, func: aggFunc, field: aggField, criteria, groupBy: groupField };
    }
    _bindParameters(command, params) {
        if (!command) return;

        // Helper to bind a value
        const bindValue = (val) => {
            if (typeof val === 'string' && val.startsWith('@')) {
                // Named parameter
                const paramName = val.substring(1); // remove @
                if (params && params.hasOwnProperty(paramName)) {
                    return params[paramName];
                } else if (Array.isArray(params)) {
                    // Fallback for array if user matched index? Unlikely for named.
                    return val;
                }
            }
            return val;
        };

        // 1. Bind Criteria (SELECT, DELETE, UPDATE, AGGREGATE)
        if (command.criteria) {
            this._info_bindCriteria(command.criteria, bindValue);
        }

        // 2. Bind Data (INSERT)
        if (command.data) {
            for (const key in command.data) {
                command.data[key] = bindValue(command.data[key]);
            }
        }

        // 3. Bind Update values (UPDATE)
        if (command.updates) {
            for (const key in command.updates) {
                command.updates[key] = bindValue(command.updates[key]);
            }
        }
    }

    _info_bindCriteria(criteria, bindFunc) {
        if (criteria.type === 'compound') {
            for (const cond of criteria.conditions) {
                this._info_bindCriteria(cond, bindFunc);
            }
        } else {
            // Single condition
            if (Array.isArray(criteria.val)) {
                criteria.val = criteria.val.map(v => bindFunc(v));
            } else {
                criteria.val = bindFunc(criteria.val);
            }
        }
    }
}

module.exports = QueryParser;
