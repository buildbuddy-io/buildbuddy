import unittest
from unittest import mock

import release


class ReleaseTest(unittest.TestCase):

    @mock.patch.object(release.requests, "get")
    def test_get_image_digest_accepts_multi_platform_images(self, get):
        get.return_value.ok = True
        get.return_value.status_code = 200
        get.return_value.headers = {
            "Docker-Content-Digest": "sha256:index",
        }

        self.assertEqual(
            "sha256:index",
            release.get_image_digest("example/project", "v1.2.3"),
        )
        get.assert_called_once_with(
            "https://gcr.io/v2/example/project/manifests/v1.2.3",
            headers={"Accept": ", ".join(release.IMAGE_MANIFEST_MEDIA_TYPES)},
        )

    @mock.patch.object(release, "create_and_push_multi_platform_manifest")
    @mock.patch.object(release, "tag_and_push_image_with_docker")
    @mock.patch.object(release, "build_image_with_bazel")
    @mock.patch.object(release, "get_image_digest", return_value=None)
    def test_push_multi_platform_image_builds_each_architecture(
        self,
        get_image_digest,
        build_image_with_bazel,
        tag_and_push_image_with_docker,
        create_and_push_multi_platform_manifest,
    ):
        release.push_multi_platform_image_for_project(
            "example/project",
            "v1.2.3",
            "//example:image",
            True,
        )

        self.assertEqual(3, get_image_digest.call_count)
        build_image_with_bazel.assert_has_calls([
            mock.call("//example:image", "//platforms:linux_x86_64"),
            mock.call("//example:image", "//platforms:linux_arm64"),
        ])
        tag_and_push_image_with_docker.assert_has_calls([
            mock.call("//example:image", "example/project", "v1.2.3-amd64"),
            mock.call("//example:image", "example/project", "v1.2.3-arm64"),
        ])
        create_and_push_multi_platform_manifest.assert_called_once_with(
            "example/project",
            "v1.2.3",
            [
                "gcr.io/example/project:v1.2.3-amd64",
                "gcr.io/example/project:v1.2.3-arm64",
            ],
        )

    @mock.patch.object(release, "run_or_die")
    def test_create_and_push_multi_platform_manifest(self, run_or_die):
        release.create_and_push_multi_platform_manifest(
            "example/project",
            "v1.2.3",
            [
                "gcr.io/example/project:v1.2.3-amd64",
                "gcr.io/example/project:v1.2.3-arm64",
            ],
        )

        run_or_die.assert_has_calls([
            mock.call(
                "docker manifest create gcr.io/example/project:v1.2.3 "
                "gcr.io/example/project:v1.2.3-amd64 "
                "gcr.io/example/project:v1.2.3-arm64"
            ),
            mock.call(
                "docker manifest push --purge gcr.io/example/project:v1.2.3"
            ),
        ])


if __name__ == "__main__":
    unittest.main()
