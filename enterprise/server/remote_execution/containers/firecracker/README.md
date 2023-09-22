# Firecracker notes

## Testing

Firecracker only runs on Linux, so when testing Firecracker,
make sure to test from a Linux machine.

To test firecracker locally, make sure to run `sudo tools/enable_local_firecracker.sh`. You will need to re-run this
every time you reboot your machine.

You can then run the executor with `--executor.enable_firecracker=true`,
then run Bazel targets with `exec_properties` configured with
`"workload-isolation-type": "firecracker"`.

If you want to test workflows locally with firecracker, make sure to set
`--app.workflows_enable_firecracker=true`.

You can also test firecracker just by running
`bazel test //enterprise/server/remote_execution/containers/firecracker:firecracker_test`. Make sure to run these tests
before submitting any changes.

## Troubleshooting

### VM crashes / `DebugMode`

You can run Firecracker in debug mode to see more detailed
VM logs, including logs from the init binary and vmexec server.

To run in debug mode, set `--executor.firecracker_debug_mode=true`
on the executor, or pass `--test_arg=--executor.firecracker_debug_mode=true`
to `bazel test`.

It's useful to use debug mode whenever the executor can't connect
to the VM (indicating the VM might have crashed).

### SSH into a VM

You can run arbitrary commands inside a VM either by running a custom
`sh_test` action or via `enterprise/tools/vmstart`. But sometimes it can
be useful to get an interactive shell within a VM to debug problems with a
regular Bazel action, such as a go test.

To enable SSH access within the VM, the VM needs to have networking
enabled (this is true by default when running the VM via the executor).
You'll also need to instrument the VM with the following setup commands,
e.g. as a setup command in your Go test action:

```shell
# Set the root password to 'root'
export PATH="$PATH:/sbin/:/usr/sbin/"
printf 'root:root' | chpasswd
# Install openssh-server (with apt, assuming the VM is running ubuntu)
apt update && apt install -y openssh-server
# Permit SSH root login
echo >>/etc/ssh/sshd_config 'PermitRootLogin yes'
# Start sshd
/etc/init.d/ssh start
```

Then, you can SSH into the VM from the host using its local IP address. If
you run `ip route` on the host, you will see two `veth*` entries like the
following:

```
192.168.0.11 via 192.168.0.14 dev veth1p6FEh
192.168.0.12/30 dev veth1p6FEh proto kernel scope link src 192.168.0.13
```

In this example, the `192.168.0.11` IP is the one you can use for SSH
access. So you can run `ssh root@192.168.0.11` with the password `root`.

### Networking issues

Occasionally, you might wind up force-killing firecracker or the executor
while a container is running, leaving the networking setup in a bad state.
If this happens, then you may need to run
`sudo tools/firecracker_clean_networking.sh`.

### Image pull issues

If `firecracker_test` is having trouble pulling docker images,
particularly from gcr.io, make sure you have run
`gcloud auth configure-docker`.

## Disk layout

There are a lot of different files involved with Firecracker containers,
and it can be hard to keep track of which files live where. The following
shows the directories relevant to a firecracker container instance:

```yaml
# --executor.build_root; c.jailerRoot; JailerConfig.ChrootBaseDir
# This is the "root" directory that everything lives underneath
# except for filecache. This is currently expected to live on the same
# device as the filecache, so that we can hardlink files from the
# filecache to here.
- /tmp/${USER}_remote_build/:
    # /executor/ contains a CAS-like dir structure that is persisted
    # across executor restarts, and does not (currently) have an
    # associated eviction mechanism. Note, this is *not* the same as the
    # filecache (--executor.local_cache_dir), which does have eviction.
    #
    # It stores the following types of files:
    # - ext4 disk images that were converted from docker images.
    #   We convert docker images to ext4 so that they can be mounted
    #   directly to microVMs. Note that container images are mounted
    #   read-only.
    # - Other static resources that are needed for firecracker VMs, e.g.
    #   linux kernel image.
    - /executor/:
        # For ext4 images, the top-level directory is the sha256 of the
        # image name string.
        #
        # See enterprise/server/util/container.go
        #
        # Ex: sha256("gcr.io/flame-public/executor-docker-default:latest")
        - /1f89a08e2136061a0bf54aaea89e47b533504b4241a5ff98d96c3dcbe04a67f3/:
            # Underneath this dir is another dir named as the SHA256 hash
            # of the image contents.
            #
            # Ex: sha256(fileContents("containerfs.ext4"))
            - /70ec2d788bcf1c7219b33554af3283c16e388bed4d7dab4e9097b8f9cd712f59/:
                - containerfs.ext4
            # There may be more than image subdir if we have multiple
            # images for the same tag. The image cache will choose
            # whichever image has the highest modified-time. In practice,
            # we don't have multiple subdirs since we don't pull images if
            # one is already cached, but this may change in the future.
            - ...
        # For other VM resources, the dir name is the sha256 of the file
        # contents and the dir contains a single file matching that sha256.
        #
        # See putFileIntoDir() in enterprise/server/util/container.go
        #
        # Ex: sha256(fileContents("vmlinux"))
        - /fc81fa0933db7977b5e1d4b9ff3a757914b579c7812b63f9cdcabc035c7057e0/:
            - vmlinux # kernel image
        - /923f71d9f8388cc9aea62cc0f52ad39d81638610914fb4c0b2b053092b02a668/:
            - initrd.cpio # initial RAM disk

    # The build root (c.jailerRoot) also contains two directories for each
    # firecracker container: a temp dir (for creating the initial
    # workspace image and scratch disk image), and a "chroot" which is the
    # chroot for jailer.
    #
    # Example temp dir:
    - /fc-container-*/: # c.tempDir; only used in Create()
        - scratchfs.ext4 # scratch disk; mounted to /. Initially empty
        - workspacefs.ext4 # workspace disk; mounted to /workspace. Initially empty

    # chroot is the path containing all files needed to start or resume
    # the VM. The jailer runs from a chroot at this directory for
    # enhanced security.
    - /firecracker/ac848787-8f81-415e-aff0-87c047f4dcde/root/: # c.getChroot()
        # /base/ is a temporary directory that exists only while we're
        # merging a diff snapshot on top of a base snapshot.
        # See SaveSnapshot() in firecracker.go
        #
        # TODO: should we put this in c.tempDir instead of the chroot?
        - /base/:
            - full-mem.snap
            # Other snapshot artifacts from the base snapshot are also
            # extracted here. Technically we don't need these, but we
            # extract them here just for simplicity.
            - ...

        # Various artifacts associated with the VM, which are
        # needed for pausing/resuming snapshots.
        #
        # When starting a VM for the first time (in Create()), we do not
        # write these files. Instead we tell the firecracker SDK where
        # these files are located, and when calling machine.Start() the
        # SDK copies the files here for us.
        #
        # When resuming a VM from snapshot, these files are expected to
        # already be here. Since we delete the chroot across runs of the
        # VM, we write these files using loader.UnpackSnapshot().
        - vmlinux # Linux kernel image
        - initrd.cpio # initial RAM disk, mounted when booting the VM
        - full-mem.snap # full memory snapshot
        - vmstate.snap # VM state snapshot (e.g. CPU registers?)
        - containerfs.ext4 # read-only container image
        - scratchfs.ext4 # scratch disk image
        - workspacefs.ext4 # workspace disk image (first run of the VM)

        # VM memory diff snapshot. This is only created when calling
        # CreateSnapshot().
        - diff-mem.snap

        - firecracker # firecracker binary
        - /run/:
            - fc.sock
            - v.sock_XXXXX
            - v.sock
        - /dev/:
            - kvm
            - urandom
            - /net/:
                - tun
# --executor.local_cache_dir: filecache root
#
# Snapshot artifacts are stored here, particularly when a firecracker
# container is not in use. The filecache may expire these artifacts,
# in which case calling Unpause() on a firecracker container will fail
# due to the artifacts being not found.
- /tmp/${USER}_filecache/:
    # The filecache contains a snapshot manifest file, which contains the
    # filecache keys for the VM snapshot artifacts, as well as the
    # serialized VM `Constants`
    #
    # See manifestData in snaploader.go
    #
    # Ex: manifestDigest(snaploader.Key(configurationHash, runnerID))
    - b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c
    - ...
```
