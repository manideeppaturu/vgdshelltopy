import xml.etree.cElementTree as ET


tree=ET.parse("/home/hduser/Desktop/sample_nestedata.xml")

root=tree.getroot()
# print(root)
root_tag=root[0].tag
# print(root_tag)
main_fields_name=root.findall(root[0].tag)[0].getchildren()
num_rows=len(root.findall(root[0].tag))
#print(main_fields_name)

print("We have to process {} rows ".format(num_rows))

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



column_names=[]

try :
    with open("/home/hduser/Desktop/sample_nestedata.txt", 'w+') as file:
        for row_num in range(0,num_rows):
            xml_row_data=[]
            row_data=""
            for main_field_name in main_fields_name:

                if (main_field_name.getchildren()):
                    child_tags=main_field_name.getchildren()
                    for child_tag in child_tags:
                        xml_row_data.append(child_tag)

                        if row_num ==0:
                            column_name=str(child_tags).split(" ")[1][1:-1]
                            column_names.append(column_name)

                else:
                    xml_row_data.append(main_field_name)
                    if row_num ==0:
                        column_name=str(main_field_name).split(" ")[1][1:-1]
                        column_names.append(column_name)


            file.write(xml_row_processing(xml_row_data)) 
except Exception as e:
    print("We have faced issue while creating delimeted file \nExcption :: "+str(e))
