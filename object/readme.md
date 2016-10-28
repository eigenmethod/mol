# $mol_object

The base class for long living objects. Every such object has an unique user friendly id like `$my_app.root(0).lister().task("123")`. This id, is a script simultaneously, by its helping we can get a link to an object from a browser console,
it's very convenience while debugging. To have a correct identificators, it's recommended to create an object through a factory wrapped by decorator [$mol_mem](../mem) at "owns" object:

```typescript
module $ {
	export class $my_app extends $mol_viewer {
	
		@ $mol_prop()
		lister() {
			return new $mol_lister()
		}
	
	}
}
```
This idetificator is used everywhere, in particular is outputted automatically while logging [logging](../log):

```
10:16:43 $my_app.root(0).lister() pull
10:16:43 $my_app.root(0).lister() push [$my_lister, undefined]
10:16:44 $my_app.root(0).lister().DOMTree() pull
10:16:44 $my_app.root(0).lister().DOMTree() push [div, undefined]
```
For outputting own messages into log, it is recommended to use `log` method:

```typescript
module $ {
	export class $my_app extends $mol_viewer {
	
		constructor() {
			this.log([ 'hello' ]) // 10:16:42 $my_app.root(0) hello
		}
	
	}
}
```
