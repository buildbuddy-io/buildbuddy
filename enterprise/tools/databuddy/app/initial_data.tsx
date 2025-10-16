import { io } from "../proto/databuddy_ts_proto";

export const initialData = ((window as any)._initialData ?? {}) as io.buildbuddy.databuddy.IInitialData;
