import { ChevronDown, ChevronsUpDown, ChevronUp } from "lucide-react";
import React from "react";
import { ClientXY } from "../../../../app/util/dom";

export type TextAlignment = "left" | "right";

export type TableProps = {
  header: string[];
  rows: any[][];
  headerTooltips: string[];
  columnAlignments: TextAlignment[];

  sort: string | null | undefined;
  sortDir: "asc" | "desc" | null | undefined;
  onSortChange: (sort: string | undefined, sortDir: "asc" | "desc" | undefined) => void;
};

type UserCellSelection = {
  startRow: number;
  startColumn: number;
  endRow: number;
  endColumn: number;
};

type TableDOMState = {
  highlightRange?: UserCellSelection;
  draggingHighlightRegion?: boolean;
};

export default React.memo(function Table({
  header,
  rows,
  columnAlignments,
  headerTooltips,
  sort,
  sortDir,
  onSortChange,
}: TableProps) {
  const tableRef = React.useRef<HTMLTableElement>(null);
  // domState holds some state about the DOM that React's VDOM doesn't know
  // about. This separate state allows fine-grained DOM updates without
  // re-rendering the entire component, which can be expensive.
  const domState = React.useRef<TableDOMState>({});

  const updateHighlightedElements = React.useMemo(
    () => (previous: UserCellSelection | null, current: UserCellSelection | null) => {
      const previousCells = new Set<string>();
      const currentCells = new Set<string>();

      if (previous) {
        for (
          let row = Math.min(previous.startRow, previous.endRow);
          row <= Math.max(previous.startRow, previous.endRow);
          row++
        ) {
          for (
            let col = Math.min(previous.startColumn, previous.endColumn);
            col <= Math.max(previous.startColumn, previous.endColumn);
            col++
          ) {
            previousCells.add(`${row}-${col}`);
          }
        }
      }
      if (current) {
        for (
          let row = Math.min(current.startRow, current.endRow);
          row <= Math.max(current.startRow, current.endRow);
          row++
        ) {
          for (
            let col = Math.min(current.startColumn, current.endColumn);
            col <= Math.max(current.startColumn, current.endColumn);
            col++
          ) {
            currentCells.add(`${row}-${col}`);
          }
        }
      }

      // Symmetric difference
      const cellsToUpdate = new Set(
        [...previousCells]
          .filter((x) => !currentCells.has(x))
          .concat([...currentCells].filter((x) => !previousCells.has(x)))
      );

      cellsToUpdate.forEach((cellKey) => {
        const [row, col] = cellKey.split("-").map(Number);
        const cellElement = tableRef.current?.rows[row]?.cells[col];
        if (!cellElement) return;
        if (currentCells.has(cellKey)) {
          cellElement.classList.add("highlighted");
        } else {
          cellElement.classList.remove("highlighted");
        }
      });
    },
    []
  );

  React.useEffect(() => {
    const windowMousedownListener = (event: MouseEvent) => {
      // TODO: on click outside of sheet, deselect cells
    };
    window.addEventListener("mousedown", windowMousedownListener);
    return () => {
      window.removeEventListener("mousedown", windowMousedownListener);
    };
  }, []);

  const onTableMouseDown = React.useCallback(
    (event: React.MouseEvent) => {
      const cell = cellFromClientXY(event);
      if (!cell) return;

      // Cell selection should only happen on left-click,
      // and when holding shift key.
      if (event.button !== 0 || !event.shiftKey) return;

      event.preventDefault();

      const highlightRange = {
        startRow: cell.row,
        startColumn: cell.column,
        endRow: cell.row,
        endColumn: cell.column,
      };
      updateHighlightedElements(domState.current.highlightRange ?? null, highlightRange);
      domState.current.highlightRange = highlightRange;

      const mousemoveListener = (e: MouseEvent) => {
        const tableRect = tableRef.current?.getBoundingClientRect();
        if (!tableRect) return;
        const cell = cellFromClientXY(clampToRect(e, tableRect));
        if (!cell) return;
        const previousRange = { ...highlightRange };
        highlightRange.endRow = cell.row;
        highlightRange.endColumn = cell.column;
        updateHighlightedElements(previousRange, highlightRange);
      };
      const mouseupListener = () => {
        window.removeEventListener("mousemove", mousemoveListener);
        domState.current.draggingHighlightRegion = false;
      };
      window.addEventListener("mousemove", mousemoveListener);
      window.addEventListener("mouseup", mouseupListener, { once: true });
    },
    [updateHighlightedElements]
  );

  const onClickHeader = React.useCallback(
    (event: React.MouseEvent<HTMLElement>) => {
      // Only handle clicks on the sort icon for now
      if (!(event.target instanceof SVGElement)) return;
      let header = event.target as HTMLElement | SVGElement | null;
      while (header && !header.dataset.columnName) {
        header = header.parentElement;
      }
      if (!header) return;

      event.preventDefault();
      event.stopPropagation();
      // const currentSort = route.params.sort;
      // const currentSortDir = route.params.sortDir;
      const columnName = event.currentTarget.dataset.columnName!;
      let newSort: string | undefined = undefined;
      let newSortDir: "asc" | "desc" | undefined = undefined;
      if (!sort) {
        newSort = columnName;
        newSortDir = "asc";
      } else if (sort === columnName) {
        if (sortDir === "asc" || !sortDir) {
          newSort = columnName;
          newSortDir = "desc";
        }
      } else if (sort !== columnName) {
        newSort = columnName;
        newSortDir = "asc";
      }
      onSortChange(newSort, newSortDir);
    },
    [sort, sortDir, onSortChange]
  );

  return (
    <table className="query-result-table" onMouseDown={onTableMouseDown} ref={tableRef}>
      <thead>
        <tr>
          {header.map((columnName, i) => (
            <th
              key={i}
              data-column-name={columnName}
              onMouseDown={onClickHeader}
              title={headerTooltips[i]}
              className={`${columnAlignments[i]}-align`}>
              <div className="table-header-content">
                <span>{columnName}</span>
                {sort === columnName ? (
                  sortDir === "desc" ? (
                    <ChevronDown className="sort-icon" />
                  ) : (
                    <ChevronUp className="sort-icon" />
                  )
                ) : (
                  <ChevronsUpDown className="sort-icon hidden" />
                )}
              </div>
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, rowIndex) => (
          <tr key={rowIndex}>
            {row.map((cell, cellIndex) => (
              <td key={cellIndex} className={`${columnAlignments[cellIndex]}-align`}>
                {cell === null ? <span className="null-value">NULL</span> : String(cell)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
});

function cellFromClientXY({ clientX, clientY }: ClientXY) {
  let cellElement = document.elementFromPoint(clientX, clientY);
  while (cellElement && cellElement.tagName !== "TD" && cellElement.tagName !== "TH") {
    cellElement = cellElement.parentElement;
  }
  if (!cellElement) return null;

  // Parent element should be a TR element
  const rowElement = cellElement.parentElement;
  if (!rowElement || rowElement.tagName !== "TR") return null;

  const rowParentElement = rowElement.parentElement;
  if (!rowParentElement) return null;

  // Get the index of the cell element within the row
  const column = Array.from(rowElement.children).indexOf(cellElement);
  if (column < 0) return null;
  const row =
    Array.from(rowParentElement.children).indexOf(rowElement) + (rowParentElement.tagName === "THEAD" ? 0 : 1);
  if (row < 0) return null;

  return { row, column, cellElement };
}

/**
 * Clamps clientX, clientY to be within boundingRect.
 */
function clampToRect({ clientX, clientY }: ClientXY, boundingRect: DOMRect): ClientXY {
  const clampedX = Math.max(boundingRect.left, Math.min(clientX, boundingRect.right - 1));
  const clampedY = Math.max(boundingRect.top, Math.min(clientY, boundingRect.bottom - 1));
  return { clientX: clampedX, clientY: clampedY };
}
