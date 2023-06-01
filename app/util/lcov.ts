/**
 * Takes an lcov file an input and returns a structured lcov object
 */
export function parseLcov(input: string) {
  const records = input.split("end_of_record");
  let items = [];
  for (let record of records) {
    let item = {
      sourceFile: "",
      testName: "",
      functionName: "",
      numFunctionsFound: 0,
      numFunctionsHit: 0,
      branchData: [] as string[],
      branchesFound: 0,
      branchesHit: 0,
      data: [] as string[],
      numLinesFound: 0,
      numLinesHit: 0,
    };
    const lines = record.split("\n");
    for (let line of lines) {
      const part = line.split(":");
      switch (part[0]) {
        case "TN":
          item.testName = part[1];
          break;
        case "SF":
          item.sourceFile = part[1];
          break;
        case "FN":
          item.functionName = part[1];
          break;
        case "FNF":
          item.numFunctionsFound = parseInt(part[1]);
          break;
        case "FNH":
          item.numFunctionsHit = parseInt(part[1]);
          break;
        case "BRDA":
          item.branchData.push(part[1]);
          break;
        case "BRF":
          item.branchesFound = parseInt(part[1]);
          break;
        case "BRH":
          item.branchesHit = parseInt(part[1]);
          break;
        case "DA":
          item.data.push(part[1]);
          break;
        case "LF":
          item.numLinesFound = parseInt(part[1]);
          break;
        case "LH":
          item.numLinesHit = parseInt(part[1]);
          break;
      }
    }
    items.push(item);
  }
  return items;
}
