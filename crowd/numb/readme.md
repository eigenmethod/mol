# CROWD Counter

Number that can be shifted by any value. Merges without lost of changes. Equivalent of dCRDT PN-Counter with same properties.

## State format

```javascript
{
	"values": [ +5, +4, -1 ],
	"stamps": [ +1001, +3002, -3003 ],
}
// Alice increases by 5.
// Bob increases by 3 then increases by 1.
// Carol decreases by 2 then increases by 1.

.value === 8
```

Stamp is always non negative.

Size = 16 * ActorsInState

## Delta format

Delta is partial state dump like:

```javascript
{
	"values": [ +4, -1 ],
	"stamps": [ +3002, -3003 ],
}
// Bob increases by 3 then increases by 1.
// Carol decreases by 2 then increases by 1.
```

Size = 16 * ActorsInDelta

## Mutations

- `shift( diff )`

## Can be reinterpreted as

- [CROWD Register](../reg) then last value wins.
- [CROWD Unordered Set](../set) as set of summands.
- [CROWD Ordered Set](../list) as set of summands.