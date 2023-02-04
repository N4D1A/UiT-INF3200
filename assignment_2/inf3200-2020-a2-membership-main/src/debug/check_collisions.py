import hashlib

bits = 16
hostnames = [
    'compute-0-0:55555','compute-0-1:55555','compute-0-2:55555','compute-0-3:55555','compute-0-4:55555','compute-1-0:55555','compute-1-1:55555','compute-2-0:55555','compute-2-1:55555','compute-2-2:55555','compute-2-3:55555','compute-3-0:55555','compute-3-1:55555','compute-3-2:55555','compute-3-3:55555','compute-3-4:55555','compute-3-5:55555','compute-3-6:55555','compute-3-7:55555','compute-3-8:55555','compute-3-9:55555','compute-3-10:55555','compute-3-11:55555','compute-3-12:55555','compute-3-13:55555','compute-3-14:55555','compute-3-15:55555','compute-3-16:55555','compute-3-17:55555','compute-3-18:55555','compute-3-19:55555','compute-3-20:55555','compute-3-21:55555','compute-3-22:55555','compute-3-23:55555','compute-3-24:55555','compute-3-25:55555','compute-3-26:55555','compute-3-27:55555','compute-3-28:55555','compute-5-0:55555','compute-5-1:55555','compute-5-2:55555','compute-6-0:55555','compute-6-1:55555','compute-6-2:55555','compute-6-3:55555','compute-6-4:55555','compute-6-5:55555','compute-6-6:55555','compute-6-7:55555','compute-6-8:55555','compute-6-9:55555','compute-6-10:55555','compute-6-11:55555','compute-6-12:55555','compute-6-13:55555','compute-6-14:55555','compute-6-15:55555','compute-6-16:55555','compute-6-17:55555','compute-6-18:55555','compute-6-19:55555','compute-6-20:55555','compute-6-21:55555','compute-6-22:55555','compute-6-23:55555','compute-6-24:55555','compute-6-25:55555','compute-6-26:55555','compute-6-27:55555','compute-6-28:55555','compute-6-29:55555','compute-6-30:55555','compute-6-31:55555','compute-6-32:55555','compute-6-33:55555','compute-6-34:55555','compute-6-35:55555','compute-6-36:55555','compute-6-37:55555','compute-6-38:55555','compute-6-39:55555','compute-6-40:55555','compute-6-41:55555','compute-6-42:55555','compute-6-43:55555','compute-6-44:55555','compute-6-45:55555','compute-6-46:55555','compute-6-47:55555','compute-6-48:55555','compute-6-49:55555','compute-6-50:55555','compute-6-51:55555','compute-6-52:55555','compute-6-53:55555','compute-6-54:55555','compute-7-0:55555','compute-7-1:55555','compute-7-2:55555','compute-7-3:55555','compute-7-4:55555','compute-7-5:55555','compute-7-6:55555','compute-7-7:55555','compute-7-8:55555','compute-8-0:55555','compute-8-1:55555','compute-8-2:55555','compute-8-3:55555','compute-8-4:55555','compute-8-5:55555','compute-8-6:55555','compute-8-7:55555','compute-8-8:55555','compute-8-9:55555','compute-8-10:55555','compute-8-11:55555','compute-8-12:55555','compute-8-13:55555','compute-8-14:55555','compute-8-15:55555'
]

def hash_fn(key, modulo):
    hasher = hashlib.sha1()
    hasher.update(bytes(key.encode("utf-8")))
    return int(hasher.hexdigest(), 16) % modulo

if __name__ == "__main__":
    
    hashed_names = []
    for name in hostnames:
        hashed = hash_fn(name, 2**bits)
        hashed_names.append(hashed)
        print("Hostname: {}\nHash: {}\n".format(name, hashed))
    hashed_names.sort()
    for hashes in hashed_names:
        print(hashes)

    if len(hashed_names) == len(set(hashed_names)):
        print("No collisions found with {} bits!".format(bits))
    else:
        print("Collisions present! Please change number of bits!")