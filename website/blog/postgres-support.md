---
slug: postgres-support
title: PostgreSQL Support for BuildBuddy
description: We're happy to announce PostgreSQL support for BuildBuddy.
author: Zoey Greer
author_title: Engineer @ BuildBuddy
date: 2023-06-21:12:00:00
author_url: https://www.github.com/tempoz
author_image_url: https://avatars.githubusercontent.com/u/735684?v=4
image: /img/blog/postgres-support.png
tags: [product]
---

By popular demand, we are introducing support for a PostgreSQL database backend for BuildBuddy! You can now use PostgreSQL as a drop-in replacement for MySQL or SQLite just by specifying a PostgreSQL endpoint to your BuildBuddy instance.

## Try it out!

```bash

# After following steps appropriate to your OS/distribution to install PostgreSQL:

# Create a PostgreSQL user corresponding to the current user if one does not already exist
sudo su postgres -c "createuser $USER --createdb --pwprompt"

# Create a PostgreSQL database called "buildbuddy_test"
createdb buildbuddy_test

# Replace "$PGPASSWORD" with the PostgreSQL password you created for your user
bb run //enterprise/server -- --database.data_source="postgresql://$USER:$PGPASSWORD@localhost/buildbuddy_test?sslmode=disable"
```

The PostgreSQL connection may also be specified via `database.advanced_data_source`, just like our other database backends, and as always these flags can instead be specified in your BuildBuddy YAML config as opposed to on the command line. Remember that when you switch out database backends, you're starting with a fresh BuildBuddy instance, so you'll need to create users and change your API keys before you can build anything against it if you're using auth.

![](/img/blog/postgres-support.png)

Questions? Comments? Other things you'd like to see in BuildBuddy? You can find us on [Slack](https://community.buildbuddy.io/) or contact us at [hello@buildbuddy.io](mailto:hello@buildbuddy.io); we'd love to hear from you!
