
def xml_row_processing(xml_row_data):
    row_data=""
    for xml_tag in xml_row_data:
        if xml_tag.text == None :
            if xml_tag != xml_row_data[-1]:
                row_data=row_data+"Null"+"|"
            else:
                row_data=row_data+"Null"
        else:
            if xml_tag != xml_row_data[-1]:
                row_data=row_data+xml_tag.text+"|"
            else:
                row_data=row_data+xml_tag.text
    row_data=row_data+"\n"
    return row_data



def xml_file_processing(input_file,output_path):
    column_names=[]
    tree=ET.parse(input_file.strip())
#     print(tree)
    root=tree.getroot()
#     print(root)
    root_tag=root[0].tag
#     print(root_tag)
    num_rows=len(root.findall(root[0].tag))
    table_name=input_file.split("/")[-1].split(".")[0].replace('-','_')
    output_file=output_path+table_name+".txt"
#     print(output_file)
    print("We have to process {} rows ".format(num_rows))
    try :
        with open(output_file, 'w+') as file:
            for row_num in range(0,num_rows):
                xml_row_data=[]
                main_fields_name=root.findall(root[0].tag)[row_num].getchildren()
                for main_field_name in main_fields_name:

                    if (main_field_name.getchildren()):
                        child_tags=main_field_name.getchildren()
                        for child_tag in child_tags:
                            xml_row_data.append(child_tag)

                            if row_num ==0:
                                column_name=str(child_tags).split(" ")[1][1:-1]
                                if column_name not in column_names:
                                    column_names.append(column_name)

                    else:
                        xml_row_data.append(main_field_name)
                        if row_num ==0:
                            column_name=str(main_field_name).split(" ")[1][1:-1]
                            if column_name not in column_names:
                                column_names.append(column_name)

                delimited_data=xml_row_processing(xml_row_data)
                print(delimited_data)
                file.write(delimited_data)
                
#         print(set(column_names)  )      
        createTable(table_name,column_names,s3_location)
    except Exception as e:
        print("We have faced issue while creating delimeted file \nExcption :: "+str(e))



try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import col, when
    import xml.etree.cElementTree as ET

    spark = SparkSession.builder.master('local[*]').appName("TableCreation").getOrCreate()

except ImportError as e:
    print("Can not import Python or Spark Modules", e)
    sys.exit(1)






