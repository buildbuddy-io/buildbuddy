function translateArgs(args) {
  let s = [];
  for (let arg of args) {
    s.push(translateArg(arg, true));
  }
  return s.join(", ");
}

function translateArg(arg, toplevel) {
  let s = "";
  switch (typeof arg) {
    case "string":
      return '"' + arg + '"';
    case "boolean":
      return arg ? "True" : "False";
    case "object":
      if (arg.builtin) {
        return arg.builtin;
      }
      if (Array.isArray(arg)) {
        let a = translateArray(arg);
        if (toplevel) return a;
        return "[" + a + "]";
      }
      let o = translateObject(arg, toplevel);
      if (toplevel) return o;
      return "{" + o + "}";
    case "function":
      return arg.ruleName;
    default:
      return "!!!unknown " + typeof arg + "!!!";
  }
  return s;
}

function translateArray(arg) {
  let s = [];
  for (let a of arg) {
    s.push(translateArg(a, false));
  }
  return s.join(", ");
}

function translateObject(arg, toplevel) {
  let s = [];
  for (let [k, v] of Object.entries(arg)) {
    let key = k;
    if (toplevel) {
      s.push(`${key} = ${translateArg(v, false)}`);
    } else {
      s.push(`"${key}": ${translateArg(v, false)}`);
    }
  }
  return s.join(", ");
}

let builtin = (name, args) => {
  return { builtin: name + "(" + translateArg(args, true) + ")" };
};
let rule = (name, args) => {
  output += name + "(" + translateArgs(args) + ")\n\n";
};

let global = this;
let output = "";
let load = (...args) => {
  output += "load(" + translateArgs(args) + ")\n\n";
  for (i = 1; i < args.length; i++) {
    let ruleName = args[i];
    global[ruleName] = (...newArgs) => rule(ruleName, newArgs);
    global[ruleName].ruleName = ruleName;
  }
};

let raw = (...args) => {
  for (let arg of args) {
    output += arg + "\n\n";
  }
};

rules.map((n) => {
  global[n] = (...args) => rule(n, args);
});

globals.map((n) => {
  global[n] = (...args) => builtin(n, args);
});
