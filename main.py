from create_tables import main as create_table_main
from etl import main as etl_main

if __name__ == "__main__":
    
    create_table_main()
    print("Inserting and processing files...")
    etl_main()
    print("Finished inserting and processing!!!")