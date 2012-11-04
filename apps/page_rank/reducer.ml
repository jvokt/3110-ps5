let (id, page_ranks) = Program.get_input() in
let values = List.map float_of_string page_ranks in
let sum = List.fold_left (+.) 0.0 values in 
Program.set_output [string_of_float sum]
