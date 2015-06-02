from tables import *  

class struct(IsDescription):
	priceAvg = Float64Col()


#writing
filename = "db/asd.h5"
fl = Filters(complevel=9, complib='blosc', shuffle=1) #enable compression  #from http://pytables.github.com/usersguide/libref/helper_classes.html#tables.Filters.complevel

h5file = openFile(filename, mode="w", title="Mt.GoxMarketDB", filters= fl)#, filters=fl)
group = h5file.createGroup("/", 'MtGox', 'Mt.Gox')
table = h5file.createTable(group, 'ticker', struct, "Readout example") 	#create table for chans
row_table = table.row

for i in xrange(1000000):
	row_table['priceAvg'] = i
	row_table.append()
table.flush()
h5file.close()



#reading
h5file = openFile(filename, mode="r", filters= fl)
for i in h5file:
	print i

mytable = h5file.root.MtGox.ticker
for i in mytable.iterrows():
	print i["priceAvg"]

h5file.close()