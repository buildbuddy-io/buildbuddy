import React from "react";
import { queriesService, useRPC } from "./rpc";
import toast from "./toast";
import { PlusCircle } from "lucide-react";
import Panel from "./components/Panel";
import { initialData } from "./initial_data";
import { io } from "../proto/databuddy_ts_proto";

/**
 * Home page component, rendering the list of queries.
 */
export default function Home() {
  const { response, error, loading } = useRPC(queriesService.getQueries, {});

  React.useEffect(() => {
    if (error) {
      toast(String(error), { status: "error" });
    } else if (loading) {
      toast("Loading", { status: "loading" });
    }
  }, [error, loading]);

  return (
    <div className="home-page">
      <div className="queries-header">
        <h1>Queries</h1>
        <div>
          <a href="/q/new" className="new-query">
            <PlusCircle />
            <span>New</span>
          </a>
        </div>
      </div>
      <QueryListPanel
        title="My queries"
        queries={response?.queries?.filter((query) => query.author === initialData.user) ?? []}
      />
      <QueryListPanel title="All queries" queries={response?.queries ?? []} />
    </div>
  );
}

type QueryListPanelProps = {
  title: string;
  queries: io.buildbuddy.databuddy.QueryMetadata[];
};

function QueryListPanel({ title, queries }: QueryListPanelProps) {
  return (
    <Panel title={title}>
      <div className="query-list">
        {queries.map((query) => (
          <a key={query.queryId} className="query-list-item" href={`/q/${query.queryId}`}>
            <span className="query-name">{query.name || `Untitled query ${query.queryId}`}</span>
            <span className="query-author">{query.author ? `${query.author}, ` : null}</span>
            <span className="query-date">
              {query.modifiedAt ? formatDate(new Date(+query.modifiedAt / 1e3)) : null}
            </span>
          </a>
        ))}
      </div>
    </Panel>
  );
}

function formatDate(date: Date) {
  const now = new Date();
  const isToday = date.toDateString() === now.toDateString();
  const hour = date.getHours() % 12 || 12;
  const minutes = date.getMinutes().toString().padStart(2, "0");
  const ampm = date.getHours() >= 12 ? "pm" : "am";
  const time = `${hour}:${minutes}${ampm}`;
  if (isToday) return time;
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const monthDay = `${months[date.getMonth()]} ${date.getDate()}`;
  if (date.getFullYear() === now.getFullYear()) return `${monthDay} at ${time}`;
  return `${monthDay}, ${date.getFullYear()} at ${time}`;
}
