import React from "react";

export function useLocalStorage(key: string) {
  const [value, setValue] = React.useState<string | null>(localStorage.getItem(key));

  const setStoredValue = React.useCallback(
    (newValue: string | null) => {
      try {
        if (newValue === null) {
          localStorage.removeItem(key);
        } else {
          localStorage.setItem(key, newValue);
        }
        setValue(newValue);
      } catch (error) {
        console.error("Error setting localStorage", error);
      }
    },
    [key]
  );

  React.useEffect(() => {
    const handleStorageChange = (event: StorageEvent) => {
      if (event.storageArea === localStorage && event.key === key && event.newValue !== value) {
        setValue(event.newValue);
      }
    };
    window.addEventListener("storage", handleStorageChange);
    return () => {
      window.removeEventListener("storage", handleStorageChange);
    };
  }, [key, value]);

  return [value, setStoredValue];
}
