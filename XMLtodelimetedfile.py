#!/usr/bin/env python


import xml.etree.cElementTree as ET



tree=ET.parse("/home/hduser/Desktop/iris.xml")

root=tree.getroot()
root_tag=root[0].tag

fields_name=root.findall(root[0].tag)[0].getchildren()

column_names=[]
for i in fields_name:
    j=str(i).split(" ")[1][1:-1]
    column_names.append(j)
    


f=open("/home/hduser/Desktop/iris1.txt", "w")
for elem in root.findall(root[0].tag):
    data_row=""
    for i in column_names:
        if elem.find(i).text == None :
            data_row=data_row+"|"+"Null"
        else:
            data_row=data_row+"|"+elem.find(i).text
                    
        
    print(data_row)
    f.write(data_row+"\n")

f.close()




