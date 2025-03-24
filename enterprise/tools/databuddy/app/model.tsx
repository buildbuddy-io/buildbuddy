import { io } from "../proto/databuddy_ts_proto";

type ClientType = "Number" | "String" | "Long";

export type ColumnModel = {
  name: string;
  databaseTypeName: string;
  clientType?: ClientType;
  data: any[];
};

export function getColumnModel(columnProto: io.buildbuddy.databuddy.IColumn): ColumnModel {
  const { data, name, databaseTypeName } = columnProto;
  let values: any[];
  let clientType: ClientType | undefined = undefined;
  if (data?.uint32Values?.length) {
    values = data.uint32Values;
    clientType = "Number";
  } else if (data?.uint64Values?.length) {
    values = data.uint64Values;
    clientType = "Long";
  } else if (data?.int32Values?.length) {
    values = data.int32Values;
    clientType = "Number";
  } else if (data?.int64Values?.length) {
    values = data.int64Values;
    clientType = "Long";
  } else if (data?.stringValues?.length) {
    values = data.stringValues;
    clientType = "String";
  } else if (data?.floatValues?.length) {
    values = data.floatValues;
    clientType = "Number";
  } else if (data?.doubleValues?.length) {
    values = data.doubleValues;
    clientType = "Number";
  } else {
    values = [];
    clientType = undefined;
  }
  // TODO: handle array values

  if (data?.nullIndexes?.length) {
    values = withNulls(values, data.nullIndexes);
  }

  return {
    name: name ?? "",
    databaseTypeName: databaseTypeName ?? "",
    clientType,
    data: values,
  };
}

function withNulls(values: any[], nullIndexes: number[]) {
  let out = [];
  let i = 0;
  let j = 0;
  while (i < values.length || j < nullIndexes.length) {
    if (j < nullIndexes.length && nullIndexes[j] === out.length) {
      out.push(null);
      j++;
    } else {
      out.push(values[i]);
      i++;
    }
  }
  return out;
}
