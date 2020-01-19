import xml.etree.cElementTree as ET
db_name="default"


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


# def createTable(tab_name,col_names,s3_location):
    
#     db_creation="CREATE DATABASE IF NOT EXISTS "+db_name
#     drop_table="DROP table if exist "+db_name+"."+tab_name
#     createTable_query="create external table "



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
                
                main_fields_name=root.findall(root[0].tag)[row_num].getchildren()
#                 print(main_fields_name)
                main_fields_name_counts=[]
                main_fields_name_cmp_counts=[]
                main_fields_name_str=str(main_fields_name)
                for i in main_fields_name:
                    j=main_fields_name_str.count(str(i).split(" ")[1][1:-1])
                    main_fields_name_cmp_counts.append(j)
                main_fields_name_cmp_counts=set(main_fields_name_cmp_counts)
#                 print(main_fields_name_cmp_counts)
                xml_row_data_rep=[]
                xml_row_data=[]
                for main_field_name in main_fields_name:
                    
                    main_fields_name_count=main_fields_name_str.count(str(main_field_name).split(" ")[1][1:-1])
                    main_fields_name_counts.append(main_fields_name_count)
#                     print(main_fields_name_count)
                    if main_fields_name_count==1 and len(main_fields_name_cmp_counts)==1:
                    
                        if (main_field_name.getchildren()):
                            child_tags=main_field_name.getchildren()
                            for child_tag in child_tags:
                                xml_row_data.append(child_tag)

                                if row_num ==0:
                                    column_name=str(child_tag).split(" ")[1][1:-1]
                                    if column_name not in column_names:
                                        column_names.append(column_name)

                        else:
                            xml_row_data.append(main_field_name)
                            if row_num ==0:
                                column_name=str(main_field_name).split(" ")[1][1:-1]
                                if column_name not in column_names:
                                    column_names.append(column_name)
                    else:
                        
                        if main_fields_name_count==1:

                            if (main_field_name.getchildren()):
                                child_tags=main_field_name.getchildren()
                                for child_tag in child_tags:
                                    xml_row_data_rep.append(child_tag)

                                    if row_num ==0:
                                        column_name=str(child_tag).split(" ")[1][1:-1]
                                        if column_name not in column_names:
                                            column_names.append(column_name)

                            else:
                                xml_row_data_rep.append(main_field_name)
                                if row_num ==0:
                                    column_name=str(main_field_name).split(" ")[1][1:-1]
                                    if column_name not in column_names:
                                        column_names.append(column_name)
                                        
                        
                        
                        if len(xml_row_data_rep)>=1 and main_fields_name_count!=1:             
                            for xml_row_data_r in xml_row_data_rep:
                                xml_row_data.append(xml_row_data_r)

                        if main_fields_name_count!=1:
                            if (main_field_name.getchildren()):
                                child_tags=main_field_name.getchildren()
                                for child_tag in child_tags:
                                    xml_row_data.append(child_tag)

                                    if row_num ==0:
                                        column_name=str(child_tag).split(" ")[1][1:-1]
                                        if column_name not in column_names:
                                            column_names.append(column_name)

                            else:
                                xml_row_data.append(main_field_name)
                                if row_num ==0:
                                    column_name=str(main_field_name).split(" ")[1][1:-1]
                                    if column_name not in column_names:
                                        column_names.append(column_name)

                        if main_fields_name_count != 1:    
    #                         print(xml_row_data)            
                            delimited_data=xml_row_processing(xml_row_data)
                            xml_row_data=[]
                            print(delimited_data)
                            file.write(delimited_data)
                            print(column_names)

                if len(set(main_fields_name_counts))==1:
#                     print(xml_row_data)
                    delimited_data=xml_row_processing(xml_row_data)
                    print(delimited_data)
                    file.write(delimited_data)
                    print(column_names)
                

#         createTable(table_name,column_names,s3_location)
    except Exception as e:
        print("We have faced issue while creating delimeted file \nExcption :: "+str(e))




xml_file_processing("/home/hduser/Desktop/xml/iris-simle.xml","/home/hduser/Desktop/xml/")

