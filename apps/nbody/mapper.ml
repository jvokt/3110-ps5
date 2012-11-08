let (body1, body2) = Program.get_input() in
let acceleration (_, l1, _) (m2, l2, _) =
  let d = Plane.s_dist l1 l2 in
  let mag = (Util.cBIG_G *. m2) /. (d *. d) in
  let dir = Plane.unit_vector l1 l2 in   
  Plane.scale_point mag dir in
let b1 = Util.unmarshal body1 in
let b2 = Util.unmarshal body2 in
let marshalled = Util.marshal (acceleration b1 b2) in 
Program.set_output [(body1, marshalled)]

