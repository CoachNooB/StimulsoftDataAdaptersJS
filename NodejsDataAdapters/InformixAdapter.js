const ibmdb = require('ibm_db');

exports.process = function (command, onResult) {
    let connection = null;

    const end = (result) => {
        try {
            if (connection) connection.closeSync();
            result.adapterVersion = "2024.3.3";
            onResult(result);
        } catch (e) {
            console.error('Error during disconnection:', e.message);
        }
    };

    const onError = (message) => {
        end({ success: false, notice: message });
    };

    try {
        const connect = () => {
            ibmdb.open(command.connectionString, (err, conn) => {
                if (err) return onError(err.message);
                connection = conn;
                onConnect();
            });
        };

        const query = (queryString, parameters, maxDataRows) => {
            connection.query(queryString, parameters, (err, result) => {
                if (err) return onError(err.message);
                onQuery(result, maxDataRows);
            });
        };

        const onConnect = () => {
            if (command.queryString) {
                if (command.command === "Execute") {
                    command.queryString = `EXECUTE PROCEDURE ${command.queryString}(${command.parameters.map(() => "?").join(", ")})`;
                }

                const { queryString, parameters } = applyQueryParameters(command.queryString, command.parameters);
                query(queryString, parameters, command.maxDataRows);
            } else {
                end({ success: true });
            }
        };

        const onQuery = (result, maxDataRows) => {
            const columns = result.length > 0 ? Object.keys(result[0]) : [];
            const types = columns.map((col, i) => mapIbmDbTypeToStandardType(result[0][col]));
            const rows = [];

            for (let i = 0; i < result.length; i++) {
                const row = [];
                for (let j = 0; j < columns.length; j++) {
                    let value = result[i][columns[j]];
                    if (Buffer.isBuffer(value)) {
                        value = value.toString('base64');
                        types[j] = "array";
                    }
                    row[j] = value;
                }
                if (maxDataRows != null && rows.length >= maxDataRows) break;
                rows.push(row);
            }

            end({ success: true, columns, rows, types });
        };

        const applyQueryParameters = (baseSqlCommand, baseParameters) => {
            const parameters = [];
            let queryString = baseSqlCommand;

            if (queryString.includes("?")) {
                baseParameters.forEach((param) => {
                    parameters.push(param.typeGroup === "number" ? +param.value : param.value);
                });
            }

            return { queryString, parameters };
        };

        const mapIbmDbTypeToStandardType = (ibmDbType) => {
            if (typeof ibmDbType === 'number') return "number";
            if (typeof ibmDbType === 'boolean') return "boolean";
            if (ibmDbType instanceof Date) return "datetime";
            if (Buffer.isBuffer(ibmDbType)) return "array";
            return "string";
        };

        connect();
    } catch (e) {
        onError(e.stack);
    }
};
