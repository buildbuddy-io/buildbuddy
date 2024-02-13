import { Line } from './logs';

export type Predicate = (e: Line) => boolean;

export function contains(m: string) {
  return (e: Line) => JSON.stringify(e).includes(m);
}

export function NOT(predicate: Predicate) {
  return (e: Line) => !predicate(e);
}

export function AND(...predicates: Predicate[]) {
  return (e: Line) => {
    for (const p of predicates) {
      if (!p(e)) return false;
    }
    return true;
  };
}

export function OR(...predicates: Predicate[]) {
  return (e: Line) => {
    for (const p of predicates) {
      if (p(e)) return true;
    }
    return false;
  };
}
