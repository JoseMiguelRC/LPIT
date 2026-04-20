import time
import os
import sys

version = "1.02"

def main():

    print("-------------------------")
    print("Live-file simulator " + version)
    print("-------------------------")
    print("")

    user = os.getlogin()

    print("Environment:")
    if sys.platform == 'linux':
        print("- OS: Ubuntu")
    elif sys.platform == 'win32':
        print("- OS: Windows")
    elif sys.platform == 'darwin':
        print("- OS: MacOS")        
    else:
        print("- OS: unknown")

    print("- User:", user)

    print("")
    
    inp_data_file = 'data_source.txt'
    out_data_file = 'data_source_live.txt'
    
    if not os.path.exists(inp_data_file):
        print("ERROR: File '" + inp_data_file + "' does not exist")
        sys.exit()
        
    if os.path.getsize(inp_data_file) == 0:
        print("ERROR: File '" + inp_data_file + "' is empty (size = 0 bytes)")   
        sys.exit()    
        
    print("- Source data file name: " + inp_data_file)
    print("- Live data file name: " + out_data_file)
    print("")

    key_int = False

    ic = 0  # Iterations counter

    # Open the output file for writing
    with open(out_data_file, 'w', encoding='utf-8') as out_file:

        try:

            while True:

                out_file.flush()

                ic += 1  # Iterations counter

                print("Iteration:", ic)
                # Open the input file for reading
                with open(inp_data_file, 'r', encoding='utf-8') as inp_file:

                    for lc, line in enumerate(inp_file):

                        lc += 1  # Line counter

                        print(f"Line #{lc} read: {line.strip()}")

                        time.sleep(1)

                        out_file.write(line)
                        out_file.flush()  # Ensure the line is written immediately

        except KeyboardInterrupt:
            key_int = True
        finally:

            out_file.close()

            print("Deleting:", out_data_file, "...")
            os.remove(out_data_file)

            if key_int:
                sys.stdout.write('\x1b[2K\n')
                print("... script gracefully stopped")
            else:
                print("... script finished")

if __name__ == "__main__":
    main()