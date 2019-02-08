import {FluxTable} from 'src/types'
import {Table, ColumnType} from 'src/minard'

export const SCHEMA_ERROR_MESSAGE = 'Encountered'

interface ToMinardTableResult {
  table: Table
  schemaErrorMessage?: string
}

export const toMinardTable = (tables: FluxTable[]): ToMinardTableResult => {
  const columns = {}
  const columnTypes = {}

  let k = 0
  let schemaErrorMessage

  for (const table of tables) {
    const header = table.data[0]

    if (!header) {
      // Ignore empty tables
      continue
    }

    for (let j = 0; j < header.length; j++) {
      const column = header[j]

      let columnConflictsSchema = false

      if (column === '' || column === 'result') {
        // Ignore these columns
        continue
      }

      const columnType = toMinardColumnType(table.dataTypes[column])

      if (columnTypes[column] && columnTypes[column] !== columnType) {
        schemaErrorMessage = `Found conflicting types for column "${column}" in response. Some values may be discarded.`
        columnConflictsSchema = true
      } else if (!columnTypes[column]) {
        columns[column] = []
        columnTypes[column] = columnType
      }

      for (let i = 1; i < table.data.length; i++) {
        // TODO: Should we omit values for all columns in this table, or just
        // the current column?
        const value = columnConflictsSchema
          ? undefined
          : parseValue(table.data[i][j].trim(), columnType)

        columns[column][k + i - 1] = value
      }
    }

    k += table.data.length - 1
  }

  const result: ToMinardTableResult = {table: {columns, columnTypes}}

  if (schemaErrorMessage) {
    result.schemaErrorMessage = schemaErrorMessage
  }

  return result
}

const TO_MINARD_COLUMN_TYPE = {
  boolean: ColumnType.Boolean,
  unsignedLong: ColumnType.Numeric,
  long: ColumnType.Numeric,
  double: ColumnType.Numeric,
  string: ColumnType.Categorical,
  'dateTime:RFC3339': ColumnType.Temporal,
}

const toMinardColumnType = (fluxDataType: string): ColumnType => {
  const columnType = TO_MINARD_COLUMN_TYPE[fluxDataType]

  if (!columnType) {
    throw new Error(`encountered unknown Flux column type ${fluxDataType}`)
  }

  return columnType
}

const parseValue = (value: string, columnType: ColumnType): any => {
  if (value === 'null') {
    return null
  }

  if (value === 'NaN') {
    return NaN
  }

  if (columnType === ColumnType.Boolean && value === 'true') {
    return true
  }

  if (columnType === ColumnType.Boolean && value === 'false') {
    return false
  }

  if (columnType === ColumnType.Categorical) {
    return value
  }

  if (columnType === ColumnType.Numeric) {
    return Number(value)
  }

  if (columnType === ColumnType.Temporal) {
    return Date.parse(value)
  }

  return null
}
