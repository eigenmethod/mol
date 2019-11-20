declare namespace $ { }
export = $;

declare namespace $ {
    namespace $$ {
        let $$: typeof $;
    }
    type $mol_ambient_context = (typeof globalThis) & (typeof $.$$) & (typeof $);
    function $mol_ambient(this: $mol_ambient_context, overrides: Partial<$mol_ambient_context>): $mol_ambient_context;
}

declare namespace $ {
    function $mol_class<Class extends any>(Class: Class): Class;
}

declare namespace $ {
    function $mol_fail(error: any): never;
}

declare namespace $ {
    function $mol_fail_hidden(error: any): never;
}

declare namespace $ {
    class $mol_object2 extends Object {
        static $: $mol_ambient_context;
        static get $$(): $mol_ambient_context;
        $: typeof $mol_object2.$;
        get $$(): $mol_ambient_context;
        constructor(init?: (obj: any) => void);
        static make<Instance>(this: new (init?: (instance: any) => void) => Instance, init?: (instance: Instance) => void): Instance;
        static toString(): any;
        destructor(): void;
        toString(): any;
        toJSON(): any;
    }
}

declare namespace $ {
    class $mol_wrapper extends $mol_object2 {
        static wrap: (task: (...ags: any[]) => any) => (...ags: any[]) => any;
        static run<Result>(task: () => Result): Result;
        static func<Args extends any[], Result, Host = void>(func: (this: Host, ...args: Args) => Result): (this: Host, ...args: Args) => Result;
        static get class(): <Class extends new (...args: any[]) => any>(Class: Class) => Class;
        static get method(): <Host, Field extends keyof Host, Args extends any[], Result>(obj: Host, name: Field, descr: TypedPropertyDescriptor<(this: Host, ...args: Args) => Result>) => TypedPropertyDescriptor<(this: Host, ...args: Args) => Result>;
    }
}

declare namespace $ {
    function $mol_dev_format_register(config: {
        header: (val: any, config: any) => any;
        hasBody: (val: any, config: any) => false;
    } | {
        header: (val: any, config: any) => any;
        hasBody: (val: any, config: any) => boolean;
        body: (val: any, config: any) => any;
    }): void;
    let $mol_dev_format_head: symbol;
    let $mol_dev_format_body: symbol;
    function $mol_dev_format_native(obj: any): any;
    function $mol_dev_format_auto(obj: any): any;
    function $mol_dev_format_element(element: string, style: object, ...content: any[]): any[];
    let $mol_dev_format_div: any;
    let $mol_dev_format_span: any;
    let $mol_dev_format_ol: any;
    let $mol_dev_format_li: any;
    let $mol_dev_format_table: any;
    let $mol_dev_format_tr: any;
    let $mol_dev_format_td: any;
    let $mol_dev_format_accent: any;
    let $mol_dev_format_strong: any;
    let $mol_dev_format_string: any;
    let $mol_dev_format_shade: any;
    let $mol_dev_format_indent: any;
}

declare namespace $ {
    function $mol_maybe<Value>(value: Value | null | undefined): Value[];
}

declare namespace $ {
    function $mol_log(path: any, ...values: any[]): void;
}

declare namespace $ {
    function $mol_log_context(next?: () => void): () => void;
}

declare namespace $ {
    function $mol_log_debug(next?: () => void): () => void;
}

declare namespace $ {
    var $mol_log_filter: (next?: string) => string;
}

declare namespace $ {
    function $mol_log_group<Task extends Function, This>(name: string, task: Task): Task;
}

declare namespace $ {
    class $mol_log2 extends $mol_wrapper {
        readonly host: any;
        readonly id: string;
        readonly args: any[];
        static current: $mol_log2;
        static wrap<This extends {
            $: $mol_ambient_context;
        }, Args extends any[], Result>(task: (this: This, ...args: Args) => Result): (this: This, ...args: Args) => any;
        constructor(host: any, id: string, args: any[]);
        stream: $mol_log2_line[];
        flush(): void;
        info(...values: any[]): void;
        static info(...values: any[]): void;
        /**
         * Enable all logs
         *
         * 	$mol_log2.excludes = []
         *
         * Exclude all atom logs:
         *
         * 	$mol_log2.excludes = [ , /˸|🠈|⏭|⏯|►|💤|☍|☌|✓|✔|✘|🕱|�/ ]
         *
         * Disable logs:
         *
         * 	$mol_log2.excludes = null
         */
        static excludes: RegExp[];
        static prefix: any[];
    }
    class $mol_log2_indent extends $mol_wrapper {
        static wrap<This extends {
            $: $mol_ambient_context;
        }, Args extends any[], Result>(task: (this: This, ...args: Args) => Result): (this: This, ...args: Args) => any;
    }
    class $mol_log2_table extends $mol_log2 {
    }
    class $mol_log2_hidden extends $mol_log2 {
        flush(): void;
    }
    class $mol_log2_line extends Array<any> {
        constructor(...items: any[]);
    }
    class $mol_log2_token extends Array<any> {
        constructor(...items: any[]);
    }
    let $mol_log2_token_empty: $mol_log2_token;
    let $mol_log2_token_indent: $mol_log2_token;
    let $mol_log2_legend: $mol_log2_table;
}

declare namespace $ {
    class $mol_after_timeout extends $mol_object2 {
        delay: number;
        task: () => void;
        id: any;
        constructor(delay: number, task: () => void);
        destructor(): void;
    }
}

declare namespace $ {
    class $mol_after_frame extends $mol_after_timeout {
        task: () => void;
        constructor(task: () => void);
    }
}

declare namespace $ {
    function $mol_compare_any(a: any, b: any): boolean;
}

declare namespace $ {
    const $mol_conform_stack: any[];
    function $mol_conform<Target, Source>(target: Target, source: Source): Target;
    const $mol_conform_handlers: WeakMap<Object, (target: any, source: any) => any>;
    function $mol_conform_handler<Class>(cl: {
        new (...args: any[]): Class;
    }, handler: (target: Class, source: Class) => Class): void;
}

declare namespace $ {
    function $mol_array_trim<Item>(array: Item[]): Item[];
}

declare namespace $ {
    const enum $mol_fiber_status {
        persist = -3,
        actual = -2,
        doubt = -1,
        obsolete = 0
    }
    function $mol_fiber_defer<Value = void>(calculate: () => Value): $mol_fiber<any>;
    function $mol_fiber_func<This, Args extends any[], Result>(calculate: (this: This, ...args: Args) => Result): (this: This, ...args: Args) => Result;
    function $mol_fiber_root<Calculate extends (this: This, ...args: any[]) => Result, Result = void, This = void>(calculate: Calculate): Calculate;
    function $mol_fiber_method<Host, Value>(obj: Host, name: keyof Host, descr: TypedPropertyDescriptor<(this: Host, ...args: any[]) => Value>): TypedPropertyDescriptor<(this: Host, ...args: any[]) => Value>;
    function $mol_fiber_async<Value>(task: () => Value): Promise<Value>;
    function $mol_fiber_sync<Args extends any[], Value = void, This = void>(request: (this: This, ...args: Args) => PromiseLike<Value>): (...args: Args) => Value;
    function $mol_fiber_warp(): Promise<void>;
    function $mol_fiber_fence(func: () => any): any;
    function $mol_fiber_unlimit<Result>(task: () => Result): Result;
    class $mol_fiber_solid extends $mol_wrapper {
        static func<This, Args extends any[], Result>(task: (this: This, ...args: Args) => Result): (this: This, ...args: Args) => Result;
    }
    class $mol_fiber<Value = any> extends $mol_wrapper {
        static wrap<Func extends (...args: any[]) => any>(task: Func): (this: ThisParameterType<Func>, ...args: Parameters<Func>) => any;
        static quant: number;
        static deadline: number;
        static liveline: number;
        static current: $mol_fiber<any>;
        static scheduled: $mol_after_frame;
        static queue: (() => PromiseLike<any>)[];
        static tick(): Promise<void>;
        static schedule(): Promise<any>;
        value: Value;
        error: Error | PromiseLike<Value>;
        cursor: $mol_fiber_status;
        masters: (number | $mol_fiber<any>)[];
        calculate: () => Value;
        schedule(): void;
        wake(): Value;
        push(value: Value): Value;
        fail(error: Error | PromiseLike<Value>): Error | PromiseLike<Value>;
        wait(promise: PromiseLike<Value>): PromiseLike<Value>;
        complete(): void;
        complete_master(master_index: number): void;
        pull(): void;
        update(): void;
        get(): Value;
        limit(): void;
        get master(): $mol_fiber;
        set master(next: $mol_fiber);
        rescue(master: $mol_fiber, master_index: number): void;
        obey(master: $mol_fiber, master_index: number): number;
        lead(slave: $mol_fiber, master_index: number): number;
        dislead(slave_index: number): void;
        disobey(master_index: number): void;
        obsolete_slaves(): void;
        obsolete(master_index: number): void;
        forget(): void;
        abort(): boolean;
        destructor(): void;
    }
    let $mol_fiber_token_runned: $mol_log2_token;
    let $mol_fiber_token_changed1: $mol_log2_token;
    let $mol_fiber_token_changed2: $mol_log2_token;
    let $mol_fiber_token_actualized: $mol_log2_token;
    let $mol_fiber_token_sleeped: $mol_log2_token;
    let $mol_fiber_token_failed: $mol_log2_token;
    let $mol_fiber_token_destructed: $mol_log2_token;
}

declare namespace $ {
    const $mol_owning_map: WeakMap<any, any>;
    function $mol_owning_allow<Having>(having: Having): having is Having & {
        destructor(): void;
    };
    function $mol_owning_get<Having, Owner extends object>(having: Having, Owner?: {
        new (): Owner;
    }): Owner | null;
    function $mol_owning_check<Owner, Having>(owner: Owner, having: Having): having is Having & {
        destructor(): void;
    };
    function $mol_owning_catch<Owner, Having>(owner: Owner, having: Having): boolean;
}

declare namespace $ {
    function $mol_atom2_value<Value>(task: () => Value): Value;
    class $mol_atom2<Value = any> extends $mol_fiber<Value> {
        static get current(): $mol_atom2<any>;
        static cached: boolean;
        static reap_task: $mol_fiber<any>;
        static reap_queue: $mol_atom2<any>[];
        static reap(atom: $mol_atom2): void;
        slaves: (number | $mol_fiber<any>)[];
        rescue(master: $mol_atom2, cursor: number): void;
        get(): Value;
        pull(): void | Value;
        _value: Value;
        get value(): Value;
        set value(next: Value);
        _error: Error | PromiseLike<Value>;
        get error(): null | Error | PromiseLike<Value>;
        set error(next: null | Error | PromiseLike<Value>);
        put(next: Value): Value;
        complete_master(master_index: number): void;
        obey(master: $mol_fiber, master_index: number): number;
        lead(slave: $mol_fiber, master_index: number): number;
        dislead(slave_index: number): void;
        obsolete(master_index?: number): void;
        doubt(master_index?: number): void;
        obsolete_slaves(): void;
        doubt_slaves(): void;
        get fresh(): (this: void) => void;
        get alone(): boolean;
        get derived(): boolean;
        destructor(): void;
    }
    let $mol_atom2_token_revalidation: $mol_log2_token;
    let $mol_atom2_token_stumbled: $mol_log2_token;
    let $mol_atom2_token_revalidated: $mol_log2_token;
    let $mol_atom2_token_leaded: $mol_log2_token;
    let $mol_atom2_token_disleaded: $mol_log2_token;
    let $mol_atom2_token_obsoleted: $mol_log2_token;
    let $mol_atom2_token_doubted: $mol_log2_token;
}

declare namespace $ {
    function $mol_const<Value>(value: Value): {
        (): Value;
        '()': Value;
    };
}

declare namespace $ {
    function $mol_atom2_field<Host extends object, Field extends keyof Host, Value extends Host[Field]>(proto: Host, name: Field, descr?: TypedPropertyDescriptor<Value>): any;
}

declare namespace $ {
    namespace $$ { }
    const $mol_object_field: unique symbol;
    class $mol_object extends Object {
        static $: $mol_ambient_context;
        static get $$(): $mol_ambient_context;
        _$: $mol_ambient_context;
        get $(): $mol_ambient_context;
        set $(next: $mol_ambient_context);
        get $$(): $mol_ambient_context;
        static make<Instance>(this: {
            new (): Instance;
        }, config: Partial<Instance>): Instance;
        static toString(): string;
        toString(): string;
        toJSON(): string;
        destructor(): void;
        [Symbol.toStringTag]: string;
    }
}

declare namespace $ {
    class $mol_window extends $mol_object {
        static size(next?: {
            width: number;
            height: number;
        }): {
            width: number;
            height: number;
        };
    }
}

declare namespace $ {
    class $mol_mem_force extends Object {
        constructor();
        $mol_mem_force: boolean;
        static $mol_mem_force: boolean;
        static toString(): string;
    }
    class $mol_mem_force_cache extends $mol_mem_force {
    }
    class $mol_mem_force_update extends $mol_mem_force {
    }
    class $mol_mem_force_fail extends $mol_mem_force_cache {
    }
}

declare namespace $ {
    function $mol_mem<Host extends object, Field extends keyof Host, Value>(proto: Host, name: Field, descr?: TypedPropertyDescriptor<(next?: Value, force?: $mol_mem_force) => Value>): any;
}

declare namespace $ {
    function $mol_dict_key(value: any): any;
    class $mol_dict<Key, Value> extends Map<Key, Value> {
        get(key: Key): Value;
        has(key: Key): boolean;
        set(key: Key, value: Value): this;
        delete(key: Key): boolean;
        forEach(back: (value: Value, key: Key, dict: Map<Key, Value>) => void, context?: any): void;
        [Symbol.iterator](): {
            [Symbol.iterator](): any;
            next(): IteratorResult<[Key, Value], any>;
        };
    }
}

declare namespace $ {
    function $mol_mem_key<Host extends object, Field extends keyof Host, Key, Value>(proto: Host, name: Field, descr?: TypedPropertyDescriptor<(key: Key, next?: Value, force?: $mol_mem_force) => Value>): any;
}

declare namespace $ {
    function $mol_atom2_autorun(calculate: () => any): $mol_atom2<unknown>;
}

/// <reference types="node" />
declare namespace $ {
    function $mol_exec(dir: string, command: string, ...args: string[]): import("child_process").SpawnSyncReturns<Buffer>;
}

interface $node {
    [key: string]: any;
}
declare var $node: $node;

declare namespace $ {
}

declare namespace $ {
    var $mol_dom_context: Window & Pick<typeof globalThis, 'Node' | 'Element' | 'HTMLElement' | 'XMLHttpRequest' | 'DOMParser' | 'XMLSerializer'>;
}

declare namespace $ {
    class $mol_defer extends $mol_object {
        run: () => void;
        constructor(run: () => void);
        destructor(): void;
        static all: $mol_defer[];
        static timer: any;
        static scheduleNative: (handler: () => void) => any;
        static schedule(): void;
        static unschedule(): void;
        static add(defer: $mol_defer): void;
        static drop(defer: $mol_defer): void;
        static run(): void;
    }
}

declare namespace $ {
    class $mol_view_selection extends $mol_object {
        static focused(next?: Element[]): Element[];
        static focus(event: FocusEvent): void;
        static blur(event: FocusEvent): void;
    }
}

declare namespace $ {
    function $mol_dom_render_attributes(el: Element, attrs: {
        [key: string]: string | number | boolean | null;
    }): void;
}

declare namespace $ {
    function $mol_fail_catch(error: object): boolean;
}

declare namespace $ {
    function $mol_dom_render_styles(el: Element, styles: {
        [key: string]: string | number;
    }): void;
}

declare namespace $ {
    function $mol_dom_render_fields(el: Element, fields: {
        [key: string]: any;
    }): void;
}

declare namespace $ {
    function $mol_dom_render_children(el: Element, childNodes: NodeList | Array<Node | string | null>): void;
}

declare namespace $ {
    function $mol_func_name(func: Function): string;
    function $mol_func_name_from<Target extends Function>(target: Target, source: Function): Target;
}

declare namespace $ {
    function $mol_deprecated<Host extends {
        constructor: Function;
    }, Method extends Function>(message: string): (host: Host, field: string, descr: TypedPropertyDescriptor<Method>) => void;
}

declare namespace $ {
    type $mol_view_content = $mol_view | Node | string | number | boolean;
    function $mol_view_visible_width(): number;
    function $mol_view_visible_height(): number;
    function $mol_view_state_key(suffix: string): string;
    class $mol_view extends $mol_object {
        static Root(id: number): $mol_view;
        autorun(): $mol_atom2<unknown>;
        static autobind(): void;
        title(): string;
        focused(next?: boolean): boolean;
        state_key(suffix?: string): string;
        dom_name(): string;
        dom_name_space(): string;
        sub(): readonly (string | number | boolean | Node | $mol_view)[];
        sub_visible(): readonly (string | number | boolean | Node | $mol_view)[];
        minimal_width(): number;
        minimal_height(): number;
        content_height(): number;
        dom_id(): string;
        dom_node(next?: Element): Element;
        dom_tree(next?: Element): Element;
        dom_node_actual(): Element;
        render(): void;
        static view_classes(): (typeof $mol_view)[];
        view_names_owned(): string[];
        view_names(): string[];
        attr_static(): {
            [key: string]: string | number | boolean | null;
        };
        attr(): {
            [key: string]: string | number | boolean | null;
        };
        style(): {
            [key: string]: string | number;
        };
        field(): {
            [key: string]: any;
        };
        event(): {
            [key: string]: (event: Event) => void;
        };
        event_async(): {
            [key: string]: (event: Event) => void;
        };
        plugins(): readonly $mol_view[];
    }
}

declare namespace $ {
    class $mol_scroll extends $mol_view {
        /**
         *  ```
         *  minimal_height 0
         *  ```
         **/
        minimal_height(): number;
        /**
         *  ```
         *  field *
         *  	^
         *  	scrollTop <= scroll_top?val
         *  	scrollLeft <= scroll_left?val
         *  	scrollBottom <= scroll_bottom?val
         *  	scrollRight <= scroll_right?val
         *  ```
         **/
        field(): {
            "scrollTop": any;
            "scrollLeft": any;
            "scrollBottom": any;
            "scrollRight": any;
        };
        /**
         *  ```
         *  scroll_top?val 0
         *  ```
         **/
        scroll_top(val?: any, force?: $mol_mem_force): any;
        /**
         *  ```
         *  scroll_left?val 0
         *  ```
         **/
        scroll_left(val?: any, force?: $mol_mem_force): any;
        /**
         *  ```
         *  scroll_bottom?val 0
         *  ```
         **/
        scroll_bottom(val?: any, force?: $mol_mem_force): any;
        /**
         *  ```
         *  scroll_right?val 0
         *  ```
         **/
        scroll_right(val?: any, force?: $mol_mem_force): any;
        /**
         *  ```
         *  event *
         *  	^
         *  	scroll?event <=> event_scroll?event
         *  ```
         **/
        event(): {
            "scroll": (event?: any) => any;
        };
        /**
         *  ```
         *  event_scroll?event null
         *  ```
         **/
        event_scroll(event?: any, force?: $mol_mem_force): any;
        /**
         *  ```
         *  Strut $mol_view style * transform <= strut_transform
         *  ```
         **/
        Strut(): $mol_view;
        /**
         *  ```
         *  strut_transform \
         *  ```
         **/
        strut_transform(): string;
    }
}

declare namespace $.$$ {
    function $mol_scroll_top(): number;
    function $mol_scroll_left(): number;
    function $mol_scroll_moving(): boolean;
    class $mol_scroll extends $.$mol_scroll {
        scroll_bottom(next?: number): number;
        scroll_right(next?: number): number;
        event_scroll(next?: Event): void;
        get $$(): $mol_ambient_context;
        strut_transform(): string;
        sub_visible(): readonly (string | number | boolean | Node | $mol_view)[];
    }
}

declare namespace $ {
    class $mol_list extends $mol_view {
        /**
         *  ```
         *  sub <= rows
         *  ```
         **/
        sub(): readonly $mol_view[];
        /**
         *  ```
         *  rows /$mol_view
         *  ```
         **/
        rows(): readonly $mol_view[];
        /**
         *  ```
         *  Empty null
         *  ```
         **/
        Empty(): any;
    }
}

declare namespace $.$$ {
    class $mol_list extends $.$mol_list {
        sub(): any[] | readonly $mol_view[];
        row_offsets(): number[];
        row_context(index: number): $mol_ambient_context;
        sub_visible(): any[] | readonly $mol_view[];
        minimal_height(): number;
    }
}

declare namespace $ {
    class $mol_perf_uibench_table extends $mol_list {
        /**
         *  ```
         *  state null
         *  ```
         **/
        state(): any;
        /**
         *  ```
         *  dom_name \table
         *  ```
         **/
        dom_name(): string;
        /**
         *  ```
         *  attr_static *
         *  	^
         *  	class \Table
         *  ```
         **/
        attr_static(): {
            "class": string;
        };
        /**
         *  ```
         *  sub <= rows
         *  ```
         **/
        sub(): readonly any[];
        /**
         *  ```
         *  rows /
         *  ```
         **/
        rows(): readonly any[];
        /**
         *  ```
         *  Row!index $mol_perf_uibench_table_row state <= row_state!index
         *  ```
         **/
        Row(index: any): $$.$mol_perf_uibench_table_row;
        /**
         *  ```
         *  row_state!index null
         *  ```
         **/
        row_state(index: any): any;
    }
}
declare namespace $ {
    class $mol_perf_uibench_table_row extends $mol_view {
        /**
         *  ```
         *  state null
         *  ```
         **/
        state(): any;
        /**
         *  ```
         *  minimal_height 18
         *  ```
         **/
        minimal_height(): number;
        /**
         *  ```
         *  dom_name \tr
         *  ```
         **/
        dom_name(): string;
        /**
         *  ```
         *  attr *
         *  	^
         *  	class <= classes
         *  	data-id <= id
         *  ```
         **/
        attr(): {
            "class": string;
            "data-id": number;
        };
        /**
         *  ```
         *  classes \TableRow
         *  ```
         **/
        classes(): string;
        /**
         *  ```
         *  id 0
         *  ```
         **/
        id(): number;
        /**
         *  ```
         *  sub /
         *  	<= Head
         *  	<= cells
         *  ```
         **/
        sub(): readonly any[];
        /**
         *  ```
         *  Head $mol_perf_uibench_table_cell text <= head_text
         *  ```
         **/
        Head(): $$.$mol_perf_uibench_table_cell;
        /**
         *  ```
         *  head_text \
         *  ```
         **/
        head_text(): string;
        /**
         *  ```
         *  cells /
         *  ```
         **/
        cells(): readonly any[];
        /**
         *  ```
         *  Cell!index $mol_perf_uibench_table_cell text <= cell_state!index
         *  ```
         **/
        Cell(index: any): $$.$mol_perf_uibench_table_cell;
        /**
         *  ```
         *  cell_state!index null
         *  ```
         **/
        cell_state(index: any): any;
    }
}
declare namespace $ {
    class $mol_perf_uibench_table_cell extends $mol_view {
        /**
         *  ```
         *  dom_name \td
         *  ```
         **/
        dom_name(): string;
        /**
         *  ```
         *  attr_static *
         *  	^
         *  	class \TableCell
         *  ```
         **/
        attr_static(): {
            "class": string;
        };
        /**
         *  ```
         *  event *
         *  	^
         *  	click?val <=> click?val
         *  ```
         **/
        event(): {
            "click": (val?: any) => any;
        };
        /**
         *  ```
         *  click?val null
         *  ```
         **/
        click(val?: any, force?: $mol_mem_force): any;
        /**
         *  ```
         *  sub / <= text
         *  ```
         **/
        sub(): readonly any[];
        /**
         *  ```
         *  text \
         *  ```
         **/
        text(): string;
    }
}

declare namespace $.$$ {
    class $mol_perf_uibench_table extends $.$mol_perf_uibench_table {
        state(): {
            items: any[];
        };
        rows_count(): number;
        rows(): $mol_perf_uibench_table_row[];
        row_state(index: number): any;
    }
    class $mol_perf_uibench_table_row extends $.$mol_perf_uibench_table_row {
        state(): {
            props: any[];
            active: boolean;
            id: number;
        };
        sub(): $mol_perf_uibench_table_cell[];
        head_text(): string;
        id(): number;
        classes(): string;
        cells_count(): number;
        cells(): $mol_perf_uibench_table_cell[];
        cell_state(index: number): any;
    }
    class $mol_perf_uibench_table_cell extends $.$mol_perf_uibench_table_cell {
        click(next?: Event): void;
    }
}

declare namespace $ {
    class $mol_perf_uibench_anim extends $mol_view {
        /**
         *  ```
         *  state null
         *  ```
         **/
        state(): any;
        /**
         *  ```
         *  attr_static *
         *  	^
         *  	class \Anim
         *  ```
         **/
        attr_static(): {
            "class": string;
        };
        /**
         *  ```
         *  sub <= boxes
         *  ```
         **/
        sub(): readonly any[];
        /**
         *  ```
         *  boxes /
         *  ```
         **/
        boxes(): readonly any[];
        /**
         *  ```
         *  Box!index $mol_perf_uibench_anim_box state <= box_state!index
         *  ```
         **/
        Box(index: any): $$.$mol_perf_uibench_anim_box;
        /**
         *  ```
         *  box_state!index null
         *  ```
         **/
        box_state(index: any): any;
    }
}
declare namespace $ {
    class $mol_perf_uibench_anim_box extends $mol_view {
        /**
         *  ```
         *  state null
         *  ```
         **/
        state(): any;
        /**
         *  ```
         *  attr *
         *  	^
         *  	class \AnimBox
         *  	data-id <= id
         *  ```
         **/
        attr(): {
            "class": string;
            "data-id": string;
        };
        /**
         *  ```
         *  id \
         *  ```
         **/
        id(): string;
        /**
         *  ```
         *  style *
         *  	^
         *  	borderRadius <= style_radius
         *  	background <= style_color
         *  ```
         **/
        style(): {
            "borderRadius": string;
            "background": string;
        };
        /**
         *  ```
         *  style_radius \
         *  ```
         **/
        style_radius(): string;
        /**
         *  ```
         *  style_color \
         *  ```
         **/
        style_color(): string;
    }
}

declare namespace $.$$ {
    class $mol_perf_uibench_anim extends $.$mol_perf_uibench_anim {
        state(): {
            items: any[];
        };
        boxes(): $mol_perf_uibench_anim_box[];
        box_state(index: number): any;
    }
    class $mol_perf_uibench_anim_box extends $.$mol_perf_uibench_anim_box {
        state(): {
            id: string;
            time: number;
        };
        id(): string;
        time(): number;
        style_radius(): string;
        style_color(): string;
    }
}

declare namespace $ {
    class $mol_perf_uibench_tree extends $mol_view {
        /**
         *  ```
         *  state null
         *  ```
         **/
        state(): any;
        /**
         *  ```
         *  attr_static *
         *  	^
         *  	class \Tree
         *  ```
         **/
        attr_static(): {
            "class": string;
        };
        /**
         *  ```
         *  sub / <= Root
         *  ```
         **/
        sub(): readonly any[];
        /**
         *  ```
         *  Root $mol_perf_uibench_tree_branch state <= root_state
         *  ```
         **/
        Root(): $$.$mol_perf_uibench_tree_branch;
        /**
         *  ```
         *  root_state null
         *  ```
         **/
        root_state(): any;
    }
}
declare namespace $ {
    class $mol_perf_uibench_tree_branch extends $mol_list {
        /**
         *  ```
         *  state null
         *  ```
         **/
        state(): any;
        /**
         *  ```
         *  dom_name \ul
         *  ```
         **/
        dom_name(): string;
        /**
         *  ```
         *  attr_static *
         *  	^
         *  	class \TreeNode
         *  ```
         **/
        attr_static(): {
            "class": string;
        };
        /**
         *  ```
         *  Branch!index $mol_perf_uibench_tree_branch state <= branch_state!index
         *  ```
         **/
        Branch(index: any): $$.$mol_perf_uibench_tree_branch;
        /**
         *  ```
         *  branch_state!index null
         *  ```
         **/
        branch_state(index: any): any;
        /**
         *  ```
         *  Leaf!index $mol_perf_uibench_tree_leaf text <= leaf_state!index
         *  ```
         **/
        Leaf(index: any): $mol_perf_uibench_tree_leaf;
        /**
         *  ```
         *  leaf_state!index null
         *  ```
         **/
        leaf_state(index: any): any;
    }
}
declare namespace $ {
    class $mol_perf_uibench_tree_leaf extends $mol_view {
        /**
         *  ```
         *  minimal_height 26
         *  ```
         **/
        minimal_height(): number;
        /**
         *  ```
         *  dom_name \li
         *  ```
         **/
        dom_name(): string;
        /**
         *  ```
         *  attr_static *
         *  	^
         *  	class \TreeLeaf
         *  ```
         **/
        attr_static(): {
            "class": string;
        };
        /**
         *  ```
         *  sub / <= text
         *  ```
         **/
        sub(): readonly any[];
        /**
         *  ```
         *  text \
         *  ```
         **/
        text(): string;
    }
}

declare namespace $.$$ {
    class $mol_perf_uibench_tree extends $.$mol_perf_uibench_tree {
        state(): {
            root: any;
        };
        root_state(): any;
    }
    class $mol_perf_uibench_tree_branch extends $.$mol_perf_uibench_tree_branch {
        state(): {
            children: any[];
        };
        sub(): $mol_view[];
        branch_state(index: number): any;
        leaf_state(index: number): any;
    }
}

declare namespace $ {
    class $mol_perf_uibench extends $mol_scroll {
        /**
         *  ```
         *  attr_static *
         *  	^
         *  	class \Main
         *  ```
         **/
        attr_static(): {
            "class": string;
        };
        /**
         *  ```
         *  sub /
         *  	<= Table
         *  	<= Anim
         *  	<= Tree
         *  ```
         **/
        sub(): readonly any[];
        /**
         *  ```
         *  Table $mol_perf_uibench_table state <= table_state
         *  ```
         **/
        Table(): $$.$mol_perf_uibench_table;
        /**
         *  ```
         *  table_state null
         *  ```
         **/
        table_state(): any;
        /**
         *  ```
         *  Anim $mol_perf_uibench_anim state <= anim_state
         *  ```
         **/
        Anim(): $$.$mol_perf_uibench_anim;
        /**
         *  ```
         *  anim_state null
         *  ```
         **/
        anim_state(): any;
        /**
         *  ```
         *  Tree $mol_perf_uibench_tree state <= tree_state
         *  ```
         **/
        Tree(): $$.$mol_perf_uibench_tree;
        /**
         *  ```
         *  tree_state null
         *  ```
         **/
        tree_state(): any;
    }
}

declare namespace $.$$ {
    class $mol_perf_uibench extends $.$mol_perf_uibench {
        state(next?: any): any;
        table_state(): any;
        anim_state(): any;
        tree_state(): any;
        location(): any;
        sub(): $mol_perf_uibench_table[] | $mol_perf_uibench_anim[] | $mol_perf_uibench_tree[];
    }
}

declare namespace $ {
    class $mol_plugin extends $mol_view {
        dom_node(next?: Element): Element;
        attr_static(): {
            [key: string]: string | number | boolean;
        };
        event(): {
            [key: string]: (event: Event) => void;
        };
        render(): void;
    }
}
