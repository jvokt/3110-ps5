(*
(print_endline "line 1 works";
let (id1, id2) = Program.get_input() in
let marshaled =  (id1, Util.marshal (0.0)) in
Program.set_output [marshaled])
*)

(print_endline "line 1 works";
let (id1, id2) = Program.get_input() in
let shared = Util.unmarshal (Program.get_shared_data()) in
let body1 = Hashtbl.find shared id1 in
let body2 = Hashtbl.find shared id2 in
let acceleration body1 body2 =
  let (_, l1, _) = body1 in
  let (m2, l2, _) = body2 in 
  let mag = (Util.cBIG_G *. m2) /. ((Plane.s_dist l1 l2)**2.0)) in
  let dir = Plane.unit_vector l1 l2 in   
  Plane.scale_point mag dir in
let marshalled = Util.marshal (acceleration body1 body2) in 
Program.set_output [(id1, marshalled)])

