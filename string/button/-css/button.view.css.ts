namespace $ { $mol_style_attach( "mol/string/button/button.view.css",
 "[mol_string_button] {\n\tbackground: transparent;\n\tcolor: var(--mol_theme_control);\n\tbox-shadow: none;\n    min-width: 0;\n\ttext-overflow: ellipsis;\n}\n\n[mol_string_button]:placeholder-shown {\n\tbackground: var(--mol_theme_field);\n\tbox-shadow: inset 0 0 0 1px var(--mol_theme_line);\n}\n\n[mol_string_button]:not(:placeholder-shown):not(:focus):enabled:hover {\n\tbackground: var(--mol_theme_hover);\n}\n\n[mol_string_button]:focus {\n\tbackground: var(--mol_theme_field);\n\tcolor: var(--mol_theme_text);\n\tbox-shadow: inset 0 0 0 1px var(--mol_theme_focus);\n}\n\n[mol_string_button]:disabled {\n\tbackground: transparent;\n\tcolor: var(--mol_theme_text);\n}\t\n\n"
) }