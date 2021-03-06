# $mol_date

Date presenter and picker.
 
## [Online demo](https://mol.js.org/app/demo/-/#demo=mol_date)

## Properties

**`date( next? : string ) : string`**

Date in YYYY-MM-DD format.
```
<= Birth_day $mol_date
	date?val <=> birth_day?val \2017-04-01
```

**`time( next? : string ) : string`**

Time in hh:mm format.
```
<= Begin $mol_date
	time?val <=> begin?val \09:00
```

**`value_number( next? : number ) : number`**

Counts of milliseconds from Unix Epoch. 
```
<= New_year $mol_date
	value_number?val <=> new_year?val 1514764800
```

**`value_moment( next? : $mol_time_moment ) : $mol_time_moment`**

Instance $mol_time_moment.
```
<= Today $mol_date
	value_moment?val <=> today?val $mol_time_moment
```

## Extends: [$mol_string](../string)
