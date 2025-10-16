package bundle

import (
	_ "embed"
)

//go:embed index.js
var IndexJS []byte

//go:embed monaco_editor.main.css
var MonacoCSS []byte

// //go:embed monaco_editor_worker_bundle.js
var MonacoWorkerJS []byte

//go:embed uPlot.min.css
var UPlotCSS []byte
