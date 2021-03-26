namespace $ {
	export class $mol_date extends $mol_pop {
		
		/**
		 * ```tree
		 * Icon $mol_icon_calendar
		 * ```
		 */
		@ $mol_mem
		Icon() {
			const obj = new this.$.$mol_icon_calendar()
			
			return obj
		}
		
		/**
		 * ```tree
		 * Anchor <= View
		 * ```
		 */
		Anchor() {
			return this.View()
		}
		
		/**
		 * ```tree
		 * bubble_content /
		 * 	<= Manual
		 * 	<= Calendar
		 * ```
		 */
		bubble_content() {
			return [
				this.Manual(),
				this.Calendar()
			] as readonly any[]
		}
		
		/**
		 * ```tree
		 * value_number?val NaN
		 * ```
		 */
		@ $mol_mem
		value_number(val?: any) {
			if ( val !== undefined ) return val
			return NaN
		}
		
		/**
		 * ```tree
		 * value_moment?val $mol_time_moment
		 * ```
		 */
		@ $mol_mem
		value_moment(val?: any) {
			if ( val !== undefined ) return val
			const obj = new this.$.$mol_time_moment()
			
			return obj
		}
		
		/**
		 * ```tree
		 * view_content /
		 * ```
		 */
		view_content() {
			return [
			] as readonly any[]
		}
		
		/**
		 * ```tree
		 * View $mol_button_minor sub <= view_content
		 * ```
		 */
		@ $mol_mem
		View() {
			const obj = new this.$.$mol_button_minor()
			
			obj.sub = () => this.view_content()
			
			return obj
		}
		
		/**
		 * ```tree
		 * date?val \
		 * ```
		 */
		@ $mol_mem
		date(val?: any) {
			if ( val !== undefined ) return val
			return ""
		}
		
		/**
		 * ```tree
		 * hint \YYYY-MM-DD
		 * ```
		 */
		hint() {
			return "YYYY-MM-DD"
		}
		
		/**
		 * ```tree
		 * enabled true
		 * ```
		 */
		enabled() {
			return true
		}
		
		/**
		 * ```tree
		 * Date_input $mol_string
		 * 	value?val <=> date?val
		 * 	hint <= hint
		 * 	enabled <= enabled
		 * 	length_max 10
		 * ```
		 */
		@ $mol_mem
		Date_input() {
			const obj = new this.$.$mol_string()
			
			obj.value = (val?: any) => this.date(val)
			obj.hint = () => this.hint()
			obj.enabled = () => this.enabled()
			obj.length_max = () => 10
			
			return obj
		}
		
		/**
		 * ```tree
		 * time?val \
		 * ```
		 */
		@ $mol_mem
		time(val?: any) {
			if ( val !== undefined ) return val
			return ""
		}
		
		/**
		 * ```tree
		 * time_hint \hh:mm
		 * ```
		 */
		time_hint() {
			return "hh:mm"
		}
		
		/**
		 * ```tree
		 * Time_input $mol_string
		 * 	value?val <=> time?val
		 * 	hint <= time_hint
		 * 	enabled <= enabled
		 * 	length_max 10
		 * ```
		 */
		@ $mol_mem
		Time_input() {
			const obj = new this.$.$mol_string()
			
			obj.value = (val?: any) => this.time(val)
			obj.hint = () => this.time_hint()
			obj.enabled = () => this.enabled()
			obj.length_max = () => 10
			
			return obj
		}
		
		/**
		 * ```tree
		 * Manual $mol_view sub /
		 * 	<= Date_input
		 * 	<= Time_input
		 * ```
		 */
		@ $mol_mem
		Manual() {
			const obj = new this.$.$mol_view()
			
			obj.sub = () => [
				this.Date_input(),
				this.Time_input()
			] as readonly any[]
			
			return obj
		}
		
		/**
		 * ```tree
		 * month_moment <= value_moment
		 * ```
		 */
		month_moment() {
			return this.value_moment()
		}
		
		/**
		 * ```tree
		 * day_selected!day false
		 * ```
		 */
		day_selected(day: any) {
			return false
		}
		
		/**
		 * ```tree
		 * day_click!day?event null
		 * ```
		 */
		@ $mol_mem_key
		day_click(day: any, event?: any) {
			if ( event !== undefined ) return event
			return null as any
		}
		
		/**
		 * ```tree
		 * prev_hint @ \Previous month
		 * ```
		 */
		prev_hint() {
			return this.$.$mol_locale.text( '$mol_date_prev_hint' )
		}
		
		/**
		 * ```tree
		 * prev?event null
		 * ```
		 */
		@ $mol_mem
		prev(event?: any) {
			if ( event !== undefined ) return event
			return null as any
		}
		
		/**
		 * ```tree
		 * Prev_icon $mol_icon_chevron_left
		 * ```
		 */
		@ $mol_mem
		Prev_icon() {
			const obj = new this.$.$mol_icon_chevron_left()
			
			return obj
		}
		
		/**
		 * ```tree
		 * Prev $mol_button_minor
		 * 	hint <= prev_hint
		 * 	click?event <=> prev?event
		 * 	sub / <= Prev_icon
		 * ```
		 */
		@ $mol_mem
		Prev() {
			const obj = new this.$.$mol_button_minor()
			
			obj.hint = () => this.prev_hint()
			obj.click = (event?: any) => this.prev(event)
			obj.sub = () => [
				this.Prev_icon()
			] as readonly any[]
			
			return obj
		}
		
		/**
		 * ```tree
		 * next_hint @ \Next month
		 * ```
		 */
		next_hint() {
			return this.$.$mol_locale.text( '$mol_date_next_hint' )
		}
		
		/**
		 * ```tree
		 * next?event null
		 * ```
		 */
		@ $mol_mem
		next(event?: any) {
			if ( event !== undefined ) return event
			return null as any
		}
		
		/**
		 * ```tree
		 * Next_icon $mol_icon_chevron_right
		 * ```
		 */
		@ $mol_mem
		Next_icon() {
			const obj = new this.$.$mol_icon_chevron_right()
			
			return obj
		}
		
		/**
		 * ```tree
		 * Next $mol_button_minor
		 * 	hint <= next_hint
		 * 	click?event <=> next?event
		 * 	sub / <= Next_icon
		 * ```
		 */
		@ $mol_mem
		Next() {
			const obj = new this.$.$mol_button_minor()
			
			obj.hint = () => this.next_hint()
			obj.click = (event?: any) => this.next(event)
			obj.sub = () => [
				this.Next_icon()
			] as readonly any[]
			
			return obj
		}
		
		/**
		 * ```tree
		 * Calendar_tools $mol_view sub /
		 * 	<= Prev
		 * 	<= Calendar_title
		 * 	<= Next
		 * ```
		 */
		@ $mol_mem
		Calendar_tools() {
			const obj = new this.$.$mol_view()
			
			obj.sub = () => [
				this.Prev(),
				this.Calendar_title(),
				this.Next()
			] as readonly any[]
			
			return obj
		}
		
		/**
		 * ```tree
		 * Calendar_title
		 * ```
		 */
		Calendar_title() {
			return this.Calendar().Title()
		}
		
		/**
		 * ```tree
		 * Calendar $mol_date_calendar
		 * 	month_moment <= month_moment
		 * 	day_selected!day <= day_selected!day
		 * 	day_click!day?event <=> day_click!day?event
		 * 	Title => Calendar_title
		 * 	head / <= Calendar_tools
		 * ```
		 */
		@ $mol_mem
		Calendar() {
			const obj = new this.$.$mol_date_calendar()
			
			obj.month_moment = () => this.month_moment()
			obj.day_selected = (day: any) => this.day_selected(day)
			obj.day_click = (day: any, event?: any) => this.day_click(day, event)
			obj.head = () => [
				this.Calendar_tools()
			] as readonly any[]
			
			return obj
		}
	}
	
	export class $mol_date_calendar extends $mol_calendar {
		
		/**
		 * ```tree
		 * day_content!day / <= Day_button!day
		 * ```
		 */
		day_content(day: any) {
			return [
				this.Day_button(day)
			] as readonly any[]
		}
		
		/**
		 * ```tree
		 * day_click!day?event null
		 * ```
		 */
		@ $mol_mem_key
		day_click(day: any, event?: any) {
			if ( event !== undefined ) return event
			return null as any
		}
		
		/**
		 * ```tree
		 * Day_button!day $mol_button_minor
		 * 	title <= day_text!day
		 * 	event_click?event <=> day_click!day?event
		 * 	minimal_height 24
		 * ```
		 */
		@ $mol_mem_key
		Day_button(day: any) {
			const obj = new this.$.$mol_button_minor()
			
			obj.title = () => this.day_text(day)
			obj.event_click = (event?: any) => this.day_click(day, event)
			obj.minimal_height = () => 24
			
			return obj
		}
	}
	
}

