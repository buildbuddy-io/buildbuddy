export function severityColor(s: string) {
  switch (s) {
    case 'DEBUG':
      return '#26A69A';
    case 'INFO':
      return '#1E88E5';
    case 'WARNING':
      return '#FB8C00';
    case 'ERROR':
      return '#E53935';
  }
}

export function severityLabel(s: string) {
  switch (s) {
    case 'DEBUG':
      return 'D';
    case 'INFO':
      return '';
    case 'WARNING':
      return 'W';
    case 'ERROR':
      return 'E';
  }
}

export function severityValue(s: string) {
  switch (s) {
    case 'DEBUG':
      return 0;
    case 'INFO':
      return 1;
    case 'WARNING':
      return 2;
    case 'ERROR':
      return 3;
    default:
      return 4;
  }
}
