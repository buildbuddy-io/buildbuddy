import { parseProfile } from "./profile_model";

const INCOMPLETE_PROFILE = `
{"otherData":{"build_id":"799cc58b-8695-4e70-a772-7a15472aa55b","output_base":"/output-base","date":"Wed Oct 26 20:24:01 UTC 2022"},"traceEvents":[
  {"name":"thread_name","ph":"M","pid":1,"tid":0,"args":{"name":"Critical Path"}},
  {"name":"thread_sort_index","ph":"M","pid":1,"tid":0,"args":{"sort_index":0}},
  {"name":"thread_name","ph":"M","pid":1,"tid":29,"args":{"name":"Main Thread"}},
  {"name":"thread_sort_index","ph":"M","pid":1,"tid":29,"args":{"sort_index":"1"}},
  {"cat":"build phase marker","name":"Launch Blaze","ph":"X","ts":-899000,"dur":899000,"pid":1,"tid":29},
  {"cat":"build phase marker","name":"Initialize command","ph":"i","ts":0,"pid":1,"tid":29},
  {"cat":"general information","name":"BazelStartupOptionsModule.beforeCommand","ph":"X","ts":200868,"dur":206,"pid":1,"tid":29}`.trim();

const COMPLETE_PROFILE = INCOMPLETE_PROFILE + "]}";

describe("parseProfile", () => {
  it("should parse a complete profile", () => {
    parseProfile(COMPLETE_PROFILE);
  });

  it("should parse an incomplete profile", () => {
    parseProfile(INCOMPLETE_PROFILE);
  });
});
