let (id, accelerations) = Program.get_input() in
let sum net acc = Plane.v_plus net (float_of_string acc)) in
let net_acc = List.fold_left sum 0.0 accelerations in
Program.set_output (id, net_acc)
