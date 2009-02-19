"""
Given a dictionary of values, appropriately define attributes in erddap's
setup.xml file


The end result is writing elements to setup.xml

"""

import os
import xml.etree.ElementTree as ET

def xmlMessage(dic):
    elem = ET.Element("erddapSetup")
    for key, value in dic.items():
        if isinstance(value, type(0)):
            ET.SubElement(elem, key, type="int").text = str(value)
        else:
            ET.SubElement(elem, key).text = value
    return ET.tostring(elem)

def xmlMessage2dict(message):
    elem = ET.XML(message)
    assert elem.tag == "erddapSetup"
    dict = {}
    for elem in elem:
        if elem.get("type") == "int":
            dict[elem.tag] = int(elem.text)
        else:
            dict[elem.tag] = elem.text
    return dict

def readSetupXML(path):
    f = open(os.path.join(path, 'setup.xml'))
    sup = f.read()
    f.close()
    return xmlMessage2dict(sup)

def writeSetupXML(path, dic):
    f = open(os.path.join(path, 'setup.xml'), 'w')
    sup = xmlMessage(dic)
    f.write(sup)



def updateSetupXML(path, dic):
    try:
        newdic = readSetupXML(path)
        newdic.update(dic)
        writeSetupXML(path, newdic)
    except Exception, inst:
        return 1
    return 0


if __name__ == '__main__':
    import sys
    path = sys.argv[1] # path where setup.xml lives
    dic = eval(sys.argv[2]) #  argv 2 should be string of dict
    print path, dic
    updateSetupXML(path, dic)


