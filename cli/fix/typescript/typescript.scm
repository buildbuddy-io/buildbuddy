; Match typical import statements
; Example:
;   import router from "../router/router";
;   import { User } from "./user";
(import_statement
	source: (string (string_fragment) @deps)
)

; Match dynamic import() calls
; Example:
;   const CodeComponent = React.lazy(() => import("../code/code"));
(call_expression
	function: (import)
	arguments: (arguments
		(string (string_fragment) @dynamic-deps)
	)
)

; Detect if @tslib is needed when compile this file
[
  "async"
  "await"
  ; '...'
  (spread_element)
  ; import * as blah from
  (import_statement
    (import_clause 
      (namespace_import)
    )
  )
] @need-tslib
