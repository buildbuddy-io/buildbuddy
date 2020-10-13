<!--
{
  "name": "RBE Platforms",
  "category": "5f18d21935ec3867907dda03",
  "priority": 700
}
-->

# RBE Platforms

## BuildBuddy default

BuildBuddy's default platform is Ubuntu 16.04 with Java 8 installed. Building on our basic command can specify this platform with the `--host_platform` flag:

```
--host_platform=@buildbuddy_toolchain//:platform
```

## Using a custom Docker image

You can configure BuildBuddy RBE to use a custom docker image, by adding the following rule to a BUILD file:

```
platform(
    name = "docker_image_platform",
    remote_execution_properties = """
        properties {
           name: "OSFamily"
           value:  "Linux"
        }
        properties {
           name: "container-image"
           value: "docker://gcr.io/YOUR:IMAGE"
        }
        """,
)
```

Make sure to replace `gcr.io/YOUR:IMAGE` with your docker image url.

You can then pass this configuration to BuildBuddy RBE with the following flag:

```
--host_platform=//:docker_image_platform
```

This assumes you've placed this rule in your root BUILD file. If you place it elsewhere, make sure to update the path accordingly.
