
from WindPy import w

w.start()
wset_astock = w.wset("sectorconstituent", f"date=20200821;sectorid=a001010100000000")

print(wset_astock)