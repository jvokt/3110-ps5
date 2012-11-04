open Util
open Plane

let (id1, id2) = Program.get_input() in
let id2body = unmarshal (Program.get_shared_data()) in
let body1 = Hashtbl.find id2body id1 in
let body2 = Hashtbl.find id2body id2 in
let acceleration body1 body2 =
  let (_, l1, _) = body1 in
  let (m2, l2, _) = body2 in 
  let mag = cBIG_G *. m2 /. ((s_dist l1 l2)**2.0)) in
  let dir = unit_vector l1 l2 in   
  scale_point mag dir
in Program.set_output (id1, acceleration body1 body2)  

