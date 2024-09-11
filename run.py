import os
import yaml
import threading
import multiprocessing
import hashlib
import zlib
import uuid
import time


# Inicijalizacija registara
# -----------------------------
# file_registry: Drzi informacije o svakom fajlu(ID,name,status)
# file_parts_registry: Drzi informacije o svakom delu fajla(part ID,fileID)
# registry_lock: Lock za obezbedjivanje registara
file_registry = {}
file_parts_registry = {}
registry_lock = threading.Lock()

# Inicijalizacija memorije
# -----------------------------
# memory_usage: Prati trenutnu potrosnju memorije
# memory_condition: Kontrolise trenutnu potrosnju memorije
memory_usage = 0
memory_condition = threading.Condition(threading.Lock())

# Funkcija za delove fajla, sluzi za kompresovanje u multiprocesingu
def process_part(file_id, part_number, block, storage_path):
    compressed = zlib.compress(block)
    digest = hashlib.md5(block).hexdigest()
    # Stavlja kompresovani deo fajla na putanju
    part_path = os.path.join(storage_path, f"{file_id}_part_{part_number}")
    with open(part_path, 'wb') as part_file:
        part_file.write(compressed)
    return part_number, digest, len(compressed)


# Funkcija za update memorije, da ne bi program prekoracio dozvoljenu memoriju
def update_memory_usage(size):
    with memory_condition:
        global memory_usage
        # Ovde se ceka ako trenutna memorija plus memorija novog dela prekoracuje dozvoljenu memoriju
        while memory_usage + size > MAX_MEMORY_USAGE:
            memory_condition.wait()
        memory_usage += size


# Funkcija za oslobadjanje memorije, kada se zavrsi sa ucitavanjem dela fajla
def release_memory(size):
    with memory_condition:
        global memory_usage
        memory_usage -= size
        memory_condition.notify_all()



def update_file_registry(file_id, data):
    with registry_lock:
        file_registry[file_id] = data


def update_file_parts_registry(part_id, data):
    with registry_lock:
        file_parts_registry[part_id] = data


def get_file_registry(file_id):
    with registry_lock:
        return file_registry.get(file_id)


def get_file_parts_registry(file_id):
    with registry_lock:
        return [part for part in file_parts_registry.values() if part['file_id'] == file_id]



active_threads = []


def accept_commands():
    global active_threads
    while True:
        cmd = input("Enter a command: \n")
        # Sacekaj da se sve niti zatvore pa tek onda se ugasi 
        if cmd == 'exit':
            for thread in active_threads:
                thread.join()
            break
        else:
            # Pravi se i pokrece nova nit za svaku komandu
            thread = threading.Thread(target=process_command, args=(cmd,))
            active_threads.append(thread)
            thread.start()


def process_command(cmd):
    if not cmd.strip():
        print("No command entered. Please enter a command.")
        return

    args = cmd.split()
    command = args[0].lower()

    # Kako se komande parsuju(hendluju)
    try:
        if command == 'put':
            if len(args) != 2:
                raise ValueError("Usage: put <file_path>")
            put_command(args[1])
        elif command == 'get':
            if len(args) != 2:
                raise ValueError("Usage: get <file_id>")
            get_command(args[1])
        elif command == 'delete':
            if len(args) != 2:
                raise ValueError("Usage: delete <file_id>")
            delete_command(args[1])
        elif command == 'list':
            if len(args) != 1:
                raise ValueError("Usage: list")
            list_command()
        else:
            print(f"Unknown command: {command}")
    except ValueError as ve:
        print(ve)
    except Exception as e:
        print(f"An error occurred: {e}")


# 'put' command: Stavlja fajl u sistem
def put_command(file_path):
    file_id = str(uuid.uuid4())  # Generise ID
    part_number = 0
    with open(file_path, 'rb') as file:
        while True:
            block = file.read(1024)  # Cita fajl u delovima
            if not block:
                break
            # Update memorije
            update_memory_usage(len(block))
            # Kompresija i smestanje file parta
            result = io_pool.apply_async(process_part, (file_id, part_number, block, STORAGE_PATH))
            part_info = result.get()
            # Oslobadjanje memorije
            release_memory(len(block))
            with registry_lock:
                # Update registar za delove fajla
                file_parts_registry[part_info[0]] = {'file_id': file_id, 'digest': part_info[1], 'size': part_info[2]}
            part_number += 1
    with registry_lock:
        # Update glavni registar fajlova nakon obrade svih delova fajla
        file_registry[file_id] = {'name': os.path.basename(file_path), 'status': 'ready', 'parts': part_number}
    print(f"File stored with ID: {file_id}\nEnter a command: ")




# 'get' command: Vraca smesteni fajl
def get_command(file_id):
    with registry_lock:
        file_info = file_registry.get(file_id)
        if not file_info or file_info['status'] != 'ready':
            print(f"File with ID {file_id} not found or not ready.\nEnter a command: ")
            return

    output_path = os.path.join(STORAGE_PATH, f"retrieved_{file_info['name']}")
    with open(output_path, 'wb') as output_file:
        for part_number in range(file_info['parts']):
            part_path = os.path.join(STORAGE_PATH, f"{file_id}_part_{part_number}")
            try:
                with open(part_path, 'rb') as part_file:
                    compressed_data = part_file.read()
                    # Dekompresuju se podaci i smestaju u output fajl
                    decompressed_data = zlib.decompress(compressed_data)
                    output_file.write(decompressed_data)
            except Exception as e:
                print(f"Error processing part {part_number} of file {file_id}: {e}\nEnter a command: ")
                return
    print(f"File {file_id} retrieved successfully.\nEnter a command: ")


# 'delete' command
def delete_command(file_id):
    with registry_lock:
        file_info = file_registry.pop(file_id, None)
        if not file_info:
            print(f"File with ID {file_id} not found.\nEnter a command: ")
            return

    for part_number in range(file_info['parts']):
        part_path = os.path.join(STORAGE_PATH, f"{file_id}_part_{part_number}")
        try:
            # Brise svaki deo fajla
            os.remove(part_path)
        except Exception as e:
            print(f"Error deleting part {part_number} of file {file_id}: {e}\nEnter a command: ")

    print(f"File {file_id} and its parts deleted successfully.\nEnter a command: ")



# 'list' command
def list_command():
    with registry_lock:
        for file_id, file_info in file_registry.items():
            print(f"ID: {file_id}, Name: {file_info['name']}, Status: {file_info['status']}")


# 'exit' command
def exit_command():
    print("Shutting down the system...")
    io_pool.close()
    io_pool.join()
    print("System shut down successfully.")



if __name__ == '__main__':
    # Ucitavanje konfiguracije 
    with open('C:/Users/JASIN/OneDrive/Desktop/Paralelni/Config.yaml','r') as file:
        config = yaml.safe_load(file)

    # Parametri
    STORAGE_PATH = config['path_to_storage']
    IO_PROCESS_COUNT = config['number_of_io_processes']
    MAX_MEMORY_USAGE = config['max_memory_usage']

    # Inicijalizacija poola
    io_pool = multiprocessing.Pool(IO_PROCESS_COUNT)

    # Pravljenje foldera
    if not os.path.exists(STORAGE_PATH):
        os.makedirs(STORAGE_PATH)

    
    accept_commands()
