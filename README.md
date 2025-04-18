Am rezolvat acest task intr-un mod simplistic.
Am ales sa rezolv task-ul in acest fel deoarece nu am mai lucrat pana in prezent in Python.

Mod de rezolvare:

- ca idee propriu-zisa am hotarat sa combin campurile produselor care au comune valorile "product_name", "product_title" si "eco_friendly" si care fac parte din aceeasi categorie de produse ("unspsc"). Astfel am redus numarul inregistrarilor cu aproximativ 2000.

- am ordonat baza de date in ordine alfabetica dupa categoria de produse ("unspsc"). Acest lucru m-a ajutat ulterior si in compararea produselor pentru a reduce din numarul de operatii. Astfel, atunci cand citesc un produs cu toate atributele sale, acesta va fi comparat cu un alt produs care se afla in aceeasi categorie. Delimitarea categoriilor a fost realizata prin salvarea pozitiilor unde se trece de la o categorie la alta in vectorul "change". Am putut crea niste valori maxime pentru "i" si "j" si o valoare minima pentru "i", astfel incat "i" sa nu mai inceapa de la 0 de fiecare data cand valorile minime/maxime se schimba.

- pentru a putea compara valorile din campurile fiecarui produs am utilizat o functie care returneaza conversia valorilor din acele campuri in string-uri. De asemenea am utilizat si o functie ("clean_duplicate") care sterge valorile ce apar de mai multe ori in acelasi camp.

Alte idei:

- initial ma gandeam la un algoritm de machine learning care sa poata depista automat produsele similare si care sa poata decide ce informatii pastreaza. Pentru datele de antrenament as fi utilizat 10% din setul de date dat. Am hotarat sa nu continui cu aceasta idee deoarece pare mult mai complicata si inca nu stapanesc suficient de bine acest limbaj de programare. De asemenea ma gandesc ca un set de date de antrenament atat de mic (aproximativ 2000) ar putea duce la un high-bias al modelului.  
