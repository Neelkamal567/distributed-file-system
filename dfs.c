#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_NODES 4
#define MAX_FILES 100
#define MAX_FILENAME 64
#define MAX_DATA 256
#define REPLICA_COUNT 3

typedef struct {
    int node_id;
    int is_active; // 1 = replica present, 0 = slot free or lost
} FileReplica;

typedef struct {
    char name[MAX_FILENAME];
    char data[MAX_DATA];
    FileReplica replicas[REPLICA_COUNT];
    int in_use; // 1 = file slot used
} FileEntry;

typedef struct {
    int id;
    int is_up; // 1 = node up, 0 = failed
} Node;

Node nodes[MAX_NODES];
FileEntry files[MAX_FILES];

// Utility: read line safely and strip newline
void read_line(char *buf, int size) {
    if (fgets(buf, size, stdin) != NULL) {
        size_t len = strlen(buf);
        if (len > 0 && buf[len - 1] == '\n') {
            buf[len - 1] = '\0';
        }
    }
}

// Count active replicas for a file
int count_active_replicas(FileEntry *f) {
    int c = 0;
    for (int i = 0; i < REPLICA_COUNT; i++) {
        if (f->replicas[i].is_active) c++;
    }
    return c;
}

// Check if given node is already a replica for this file
int is_node_already_replica(FileEntry *f, int node_id) {
    for (int i = 0; i < REPLICA_COUNT; i++) {
        if (f->replicas[i].is_active && f->replicas[i].node_id == node_id) {
            return 1;
        }
    }
    return 0;
}

// Initialize nodes and file table
void init_system() {
    for (int i = 0; i < MAX_NODES; i++) {
        nodes[i].id = i;
        nodes[i].is_up = 1; // all nodes up initially
    }
    for (int i = 0; i < MAX_FILES; i++) {
        files[i].in_use = 0;
    }
}

// Find file index by name, -1 if not found
int find_file_index(const char *name) {
    for (int i = 0; i < MAX_FILES; i++) {
        if (files[i].in_use && strcmp(files[i].name, name) == 0) {
            return i;
        }
    }
    return -1;
}

// Create replicas for a file on currently UP nodes.
// Returns number of replicas created.
int create_replicas(FileEntry *f) {
    int created = 0;
    for (int i = 0; i < REPLICA_COUNT; i++) {
        f->replicas[i].is_active = 0;
        f->replicas[i].node_id = -1;
    }

    for (int n = 0; n < MAX_NODES && created < REPLICA_COUNT; n++) {
        if (nodes[n].is_up) {
            f->replicas[created].node_id = n;
            f->replicas[created].is_active = 1;
            created++;
        }
    }
    return created;
}

// Try to restore replication factor after a node failure
void heal_replication() {
    for (int i = 0; i < MAX_FILES; i++) {
        if (!files[i].in_use) continue;

        FileEntry *f = &files[i];
        int active = count_active_replicas(f);

        while (active < REPLICA_COUNT) {
            int found = 0;
            for (int n = 0; n < MAX_NODES; n++) {
                if (!nodes[n].is_up) continue;
                if (is_node_already_replica(f, n)) continue;

                for (int r = 0; r < REPLICA_COUNT; r++) {
                    if (!f->replicas[r].is_active) {
                        f->replicas[r].is_active = 1;
                        f->replicas[r].node_id = n;
                        active++;
                        printf("[HEAL] File '%s' replicated to node %d to maintain fault tolerance.\n",
                               f->name, n);
                        break;
                    }
                }
                if (found) break;
            }
            if (!found) {
                // No more nodes available to create new replicas
                break;
            }
        }
    }
}

// Create a new distributed file
void create_file() {
    char name[MAX_FILENAME];
    char data[MAX_DATA];

    int ch;
    // clear leftover newline from previous scanf
    while ((ch = getchar()) != '\n' && ch != EOF) {
        // discard
    }

    printf("Enter file name: ");
    read_line(name, sizeof(name));

    if (find_file_index(name) != -1) {
        printf("File with this name already exists.\n");
        return;
    }

    printf("Enter file data (single line): ");
    read_line(data, sizeof(data));

    // Find free slot
    int index = -1;
    for (int i = 0; i < MAX_FILES; i++) {
        if (!files[i].in_use) {
            index = i;
            break;
        }
    }
    if (index == -1) {
        printf("File table full. Cannot create more files.\n");
        return;
    }

    FileEntry *f = &files[index];
    strncpy(f->name, name, MAX_FILENAME - 1);
    f->name[MAX_FILENAME - 1] = '\0';
    strncpy(f->data, data, MAX_DATA - 1);
    f->data[MAX_DATA - 1] = '\0';
    f->in_use = 1;

    int replicas = create_replicas(f);
    if (replicas == 0) {
        printf("No UP nodes available. File cannot be stored.\n");
        f->in_use = 0;
        return;
    } else if (replicas < REPLICA_COUNT) {
        printf("File stored, but only %d replicas created (needed %d).\n",
               replicas, REPLICA_COUNT);
    } else {
        printf("File stored with %d replicas.\n", replicas);
    }
}

// Read file (client side) with fault tolerance
void read_file() {
    char name[MAX_FILENAME];

    int ch;
    while ((ch = getchar()) != '\n' && ch != EOF) {
        // discard
    }

    printf("Enter file name to read: ");
    read_line(name, sizeof(name));

    int idx = find_file_index(name);
    if (idx == -1) {
        printf("File not found.\n");
        return;
    }

    FileEntry *f = &files[idx];

    // Try to find a live replica
    for (int i = 0; i < REPLICA_COUNT; i++) {
        if (!f->replicas[i].is_active) continue;

        int node_id = f->replicas[i].node_id;
        if (node_id >= 0 && node_id < MAX_NODES && nodes[node_id].is_up) {
            printf("File '%s' read from node %d.\n", f->name, node_id);
            printf("Data: %s\n", f->data);
            return;
        }
    }

    printf("All replicas are on FAILED nodes. Data temporarily unavailable.\n");
}

// List all files and their replica locations
void list_files() {
    printf("\n=== Files & Replicas ===\n");
    for (int i = 0; i < MAX_FILES; i++) {
        if (!files[i].in_use) continue;

        FileEntry *f = &files[i];
        printf("File: %s\n", f->name);
        printf("  Data: %s\n", f->data);
        printf("  Replicas: ");
        for (int r = 0; r < REPLICA_COUNT; r++) {
            if (f->replicas[r].is_active) {
                int nid = f->replicas[r].node_id;
                printf("[node %d%s] ",
                       nid,
                       nodes[nid].is_up ? " UP" : " DOWN");
            }
        }
        printf("\n");
    }
    printf("========================\n");
}

// Show status of all nodes
void list_nodes() {
    printf("\n=== Node Status ===\n");
    for (int i = 0; i < MAX_NODES; i++) {
        printf("Node %d : %s\n", nodes[i].id,
               nodes[i].is_up ? "UP" : "DOWN");
    }
    printf("===================\n");
}

// Simulate node failure
void fail_node() {
    int id;
    printf("Enter node id to FAIL (0 to %d): ", MAX_NODES - 1);
    if (scanf("%d", &id) != 1) {
        printf("Invalid input.\n");
        return;
    }
    if (id < 0 || id >= MAX_NODES) {
        printf("Invalid node id.\n");
        return;
    }
    if (!nodes[id].is_up) {
        printf("Node %d already DOWN.\n", id);
        return;
    }

    nodes[id].is_up = 0;
    printf("Node %d marked as DOWN.\n", id);

    // Mark replicas on this node as inactive
    for (int i = 0; i < MAX_FILES; i++) {
        if (!files[i].in_use) continue;
        for (int r = 0; r < REPLICA_COUNT; r++) {
            if (files[i].replicas[r].is_active &&
                files[i].replicas[r].node_id == id) {
                files[i].replicas[r].is_active = 0;
            }
        }
    }

    // Try to heal replication to maintain fault tolerance
    heal_replication();
}

// Recover node (bring back UP)
void recover_node() {
    int id;
    printf("Enter node id to RECOVER (0 to %d): ", MAX_NODES - 1);
    if (scanf("%d", &id) != 1) {
        printf("Invalid input.\n");
        return;
    }
    if (id < 0 || id >= MAX_NODES) {
        printf("Invalid node id.\n");
        return;
    }
    if (nodes[id].is_up) {
        printf("Node %d is already UP.\n", id);
        return;
    }
    nodes[id].is_up = 1;
    printf("Node %d is now UP.\n", id);

    // After recovery we could also rebalance, but our heal_replication()
    // already runs on failures. You can call it here again if you want
    // to rebalance replicas to use newly available node.
    heal_replication();
}

void print_menu() {
    printf("\n================ Distributed File System Simulator ================\n");
    printf("1. Create file (with replication)\n");
    printf("2. Read file (fault tolerant)\n");
    printf("3. List files & replicas\n");
    printf("4. Show node status\n");
    printf("5. Simulate node FAILURE\n");
    printf("6. Recover node\n");
    printf("0. Exit\n");
    printf("==================================================================\n");
    printf("Enter choice: ");
}

int main() {
    int choice;
    init_system();

    while (1) {
        print_menu();
        if (scanf("%d", &choice) != 1) {
            printf("Invalid input, exiting.\n");
            break;
        }

        switch (choice) {
            case 1:
                create_file();
                break;
            case 2:
                read_file();
                break;
            case 3:
                list_files();
                break;
            case 4:
                list_nodes();
                break;
            case 5:
                fail_node();
                break;
            case 6:
                recover_node();
                break;
            case 0:
                printf("Exiting.\n");
                return 0;
            default:
                printf("Invalid choice. Try again.\n");
                break;
        }
    }

    return 0;
}
// Feature 1: Node status
//Feature 2: File replication
//Feature 3: Node failure simulation
//Feature 4: Fault tolerance


