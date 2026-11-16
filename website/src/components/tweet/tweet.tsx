// Simplified from https://react-tweet.vercel.app/twitter-theme/advanced

import { EmbeddedTweet, TweetNotFound, TweetSkeleton, useTweet } from "react-tweet";
import "react-tweet/theme.css";

export const Tweet = ({ id }: { id: string }) => {
  const { data, error, isLoading } = useTweet(id);

  if (isLoading) {
    return (
      <div style={{ display: "flex", justifyContent: "center" }}>
        <TweetSkeleton />
      </div>
    );
  }
  if (error || !data) {
    return (
      <div style={{ display: "flex", justifyContent: "center" }}>
        <TweetNotFound error={error} />
      </div>
    );
  }

  return (
    <div style={{ display: "flex", justifyContent: "center" }}>
      <EmbeddedTweet tweet={data} />
    </div>
  );
};
