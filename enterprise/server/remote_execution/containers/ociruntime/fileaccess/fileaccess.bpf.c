//go:build ignore

// Records regular files opened for reading by processes in tracked cgroups.
//
// Built without kernel or libbpf headers: the kernel types below are minimal
// CO-RE definitions whose field offsets are relocated at load time using the
// running kernel's BTF.

typedef unsigned char __u8;
typedef unsigned int __u32;
typedef unsigned long long __u64;

#define SEC(name) __attribute__((section(name), used))
#define __uint(name, val) int (*name)[val]
#define __type(name, val) typeof(val) *name

// Values from include/uapi/linux/bpf.h.
#define BPF_MAP_TYPE_HASH 1
#define BPF_MAP_TYPE_PERCPU_ARRAY 6
#define BPF_MAP_TYPE_RINGBUF 27
#define BPF_NOEXIST 1
#define BPF_F_NO_PREALLOC 1

static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *)1;
static long (*bpf_map_update_elem)(void *map, const void *key, const void *value, __u64 flags) = (void *)2;
static long (*bpf_map_delete_elem)(void *map, const void *key) = (void *)3;
static __u64 (*bpf_get_current_ancestor_cgroup_id)(int level) = (void *)123;
static long (*bpf_ringbuf_output)(void *ringbuf, void *data, __u64 size, __u64 flags) = (void *)130;
static long (*bpf_d_path)(void *path, char *buf, __u32 sz) = (void *)147;

#define FMODE_READ 0x1
#define S_IFMT 0170000
#define S_IFREG 0100000
#define PATH_MAX 4096
#define MAX_CGROUP_DEPTH 32

struct super_block {
  __u32 s_dev;
} __attribute__((preserve_access_index));

struct inode {
  unsigned short i_mode;
  unsigned long i_ino;
  struct super_block *i_sb;
} __attribute__((preserve_access_index));

struct path {
  void *mnt;
  void *dentry;
} __attribute__((preserve_access_index));

struct file {
  struct path f_path;
  struct inode *f_inode;
  unsigned int f_mode;
} __attribute__((preserve_access_index));

// Identifies one path opened within one tracked cgroup. The dentry
// distinguishes hardlinks that share an inode.
struct key {
  __u64 cgroup_id;
  __u64 dentry;
  __u64 ino;
};

struct event {
  struct key key;
  __u32 path_len;  // Includes the trailing NUL.
  char path[PATH_MAX];
};

// Cgroup IDs whose processes (including those in descendant cgroups) are
// recorded. Managed from userspace.
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 4096);
  __type(key, __u64);
  __type(value, __u8);
} tracked SEC(".maps");

// Keys already reported, so that each path is sent to userspace once per
// recording. Userspace deletes a recording's keys when it stops.
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, 1 << 21);
  __uint(map_flags, BPF_F_NO_PREALLOC);
  __type(key, struct key);
  __type(value, __u8);
} seen SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
  __uint(max_entries, 1);
  __type(key, __u32);
  __type(value, struct event);
} scratch SEC(".maps");

struct {
  __uint(type, BPF_MAP_TYPE_RINGBUF);
  __uint(max_entries, 16 << 20);
} events SEC(".maps");

SEC("fentry/security_file_open")
int file_open(__u64 *ctx) {
  struct file *file = (struct file *)ctx[0];
  if (!(file->f_mode & FMODE_READ)) {
    return 0;
  }
  if ((file->f_inode->i_mode & S_IFMT) != S_IFREG) {
    return 0;
  }

  __u64 cgroup_id = 0;
  for (int level = 1; level < MAX_CGROUP_DEPTH; level++) {
    __u64 id = bpf_get_current_ancestor_cgroup_id(level);
    if (id == 0) {
      break;
    }
    if (bpf_map_lookup_elem(&tracked, &id)) {
      cgroup_id = id;
      break;
    }
  }
  if (cgroup_id == 0) {
    return 0;
  }

  __u32 zero = 0;
  struct event *e = bpf_map_lookup_elem(&scratch, &zero);
  if (!e) {
    return 0;
  }
  e->key.cgroup_id = cgroup_id;
  e->key.dentry = (__u64)file->f_path.dentry;
  e->key.ino = file->f_inode->i_ino;

  __u8 one = 1;
  // Skip if already reported, or if the map is full (rather than reporting
  // every open).
  if (bpf_map_update_elem(&seen, &e->key, &one, BPF_NOEXIST) != 0) {
    return 0;
  }
  long n = bpf_d_path(&file->f_path, e->path, sizeof(e->path));
  if (n <= 0 || n > PATH_MAX) {
    bpf_map_delete_elem(&seen, &e->key);
    return 0;
  }
  e->path_len = n;
  __u64 size = __builtin_offsetof(struct event, path) + (__u64)n;
  if (size > sizeof(*e)) {
    return 0;
  }
  if (bpf_ringbuf_output(&events, e, size, 0) != 0) {
    // Ring buffer full: forget the key so a later open can report it.
    bpf_map_delete_elem(&seen, &e->key);
  }
  return 0;
}

// "Dual MIT/GPL" is GPL-compatible, which tracing programs and bpf_d_path
// require.
char LICENSE[] SEC("license") = "Dual MIT/GPL";
