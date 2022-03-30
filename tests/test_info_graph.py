import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *

def SetupGraph():
    print()
    g = Graph()
    with Transaction(g):
        for k in range(13456):
            instantiate(ET.Machine, g)
    with Transaction(g):
        instantiate(ET.ProcessOrder, g)
        instantiate(ET.Machine, g)
        m = instantiate(ET.Machine, g)

        scr = instantiate(ET.ZEF_Script, g)
        s = instantiate(AET.String, g)
        instantiate(scr, RT.ZEF_Python, s, g)

    return g,m,scr,s
    
class MyTestCase(unittest.TestCase):
    def test_return_types(self):
        g,m,scr,s = SetupGraph()
        
        m | now | yo[False] | collect
        g | yo[False] | collect
        
if __name__ == '__main__':
    unittest.main()
