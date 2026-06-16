// Using Compiled Result from upload asset script
// Both CCD and CH images can be generated using this script. Comment out CCD if you
// are generating CH image and vice versa

// TODO: Set total_files according to the number of result files for a particular ACZ
// in a particular year
var total_files = 5;

// TODO: Mention the year for which you want to run this script for.
var year = '2018';

// TODO: Uncomment the acz for which you want to run this script for.
// var acz = 'Eastern Plateau & Hills Region';
// var acz = 'Middle Gangetic Plain Region';
// var acz = 'Lower Gangetic Plain Region';
// var acz = 'Western Himalayan Region';
var acz = 'Eastern Himalayan Region';
// var acz = 'Upper Gangetic Plain Region';
// var acz = 'Trans Gangetic Plain Region';
// var acz = 'Central Plateau & Hills Region';
// var acz = 'Western Plateau and Hills Region';
// var acz = 'Southern Plateau and Hills Region';
// var acz = 'East Coast Plains & Hills Region';

var res_list = [];
for (var i=0; i<total_files; i++) {
  res_list.push('_result_' + String(i) + '_');
}

var agroclimaticZoneAcronymDict = {
  'Eastern Plateau & Hills Region': 'EPAHR',
  'Southern Plateau and Hills Region': 'SPAHR',
  'East Coast Plains & Hills Region': 'ECPHR',
  'Western Plateau and Hills Region': 'WPAHR',
  'Central Plateau & Hills Region': 'CPAHR',
  'Lower Gangetic Plain Region': 'LGPR',
  'Middle Gangetic Plain Region': 'MGPR',
  'Upper Gangetic Plain Region': 'UGPR',
  'Trans Gangetic Plain Region': 'TGPR',
  'Eastern Himalayan Region': 'EHR',
  'Western Himalayan Region': 'WHR'
};

var acronym = agroclimaticZoneAcronymDict[acz];

var india_boundary = ee.FeatureCollection("projects/ext-datasets/assets/datasets/ACZs");
var aoi = india_boundary.filter(ee.Filter.eq('regionname', acz)).geometry();
print(aoi)
var project_path = "projects/corestack1-dev-alpha/assets/tree_characteristics";
var outpath_path = "projects/corestack1-dev-alpha/assets/tree_characteristics";

// ccd
for (var k=0; k<res_list.length; k++) {

  var res = res_list[k];

  var fc = ee.FeatureCollection(project_path + '/ccd_' + year + res + acronym);

  var month = ['cc_1', 'cc_2', 'cc_3', 'cc_4', 'cc_5', 'cc_6', 'cc_7', 'cc_8', 'cc_9', 'cc_10', 'cc_11', 'cc_12', 'cc'];

  var district_img = ee.Image(0);

  for (var i=0; i<13; i++) {

    var image = fc.reduceToImage({
      // List of properties to convert to bands
      properties: [month[i]],

      // Reducer function (optional, default is 'first')
      reducer: ee.Reducer.first() // Chooses the value from the first feature for each pixel
    });

    image = image.rename([month[i]]);
    // print(image);

    district_img = district_img.addBands(image);
  }

  district_img = district_img.reproject('EPSG:4326', null, 25).clip(aoi).select(month);
  print(district_img);

  Export.image.toAsset({
    image: district_img,
    description: 'fc_to_image_ccd_' + year + '_' + acronym,
    assetId: outpath_path+ '/ccd_'+ year + '/ccd_' + year + res + acronym,  // <> modify these
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });
}



// ch

var dist_list_dict = {'EPAHR': ['Baloda Bazar', 'Balod', 'BalrampurC', 'Bastar', 'Bemetara', 'BijapurC', 'BilaspurC', 'Dantewada', 'Dhamtari', 'Durg', 'Gariaband', 'Janjgir-Champa', 'Jashpur', 'Kabeerdham', 'Kondagaon', 'Korba', 'Koriya', 'Mahasamund', 'Mungeli', 'Narayanpur', 'Raigarh', 'Raipur', 'Rajnandgaon', 'Sukma', 'Surajpur', 'Surguja', 'Uttar Bastar Kanker', 'Bokaro', 'Chatra', 'Deoghar', 'Dhanbad', 'Dumka', 'Garhwa', 'Giridih', 'Gumla', 'Hazaribagh', 'Jamtara', 'Khunti', 'Kodarma', 'Latehar', 'Lohardaga', 'Pakur', 'Palamu', 'Pashchimi Singhbhum', 'Purbi Singhbhum', 'Ramgarh', 'Ranchi', 'Saraikela-kharsawan', 'Simdega', 'Anuppur', 'Balaghat', 'Shahdol', 'Sidhi', 'Singrauli', 'Umaria', 'Bhandara', 'Chandrapur', 'Garhchiroli', 'Gondiya', 'Anugul', 'Balangir', 'Bargarh', 'Bauda', 'Debagarh', 'Dhenkanal', 'Jharsuguda', 'Kalahandi', 'Kandhamal', 'Kendujhar', 'Koraput', 'Malkangiri', 'Mayurbhanj', 'Nabarangapur', 'Nuapada', 'Rayagada', 'Sambalpur', 'Subarnapur', 'Sundargarh', 'Puruliya'],
                      'CPAHR': ['Ashoknagar', 'Betul', 'Bhind', 'Bhopal', 'Chhatarpur', 'Chhindwara', 'Damoh', 'Datia', 'Dindori', 'Guna', 'Gwalior', 'Harda', 'Hoshangabad', 'Jabalpur', 'Katni', 'Mandla', 'Morena', 'Narsimhapur', 'Panna', 'Raisen', 'Rajgarh', 'Rewa', 'Sagar', 'Satna', 'Sehore', 'Seoni', 'Sheopur', 'Shivpuri', 'Tikamgarh', 'Vidisha', 'Ajmer', 'Alwar', 'Banswara', 'Baran', 'Bharatpur', 'Bhilwara', 'Bundi', 'Chittaurgarh', 'Dausa', 'Dhaulpur', 'Dungarpur', 'Jaipur', 'Karauli', 'Kota', 'Pali', 'Pratapgarhraj', 'Rajsamand', 'Sawai Madhopur', 'Sirohi', 'Tonk', 'Udaipur', 'Banda', 'Chitrakoot', 'Hamirpurup', 'Jalaun', 'Jhansi', 'Lalitpur', 'Mahoba'],
                      'SPAHR': ['Anantapur', 'Chittoor', 'Kurnool', 'Y.S.R.', 'Bagalkot', 'Bangalore Rural', 'Bangalore', 'Belgaum', 'Bellary', 'Bidar', 'Bijapur', 'Chamrajnagar', 'Chikballapura', 'Chitradurga', 'Davanagere', 'Dharwad', 'Gadag', 'Gulbarga', 'Hassan', 'Haveri', 'Kolar', 'Koppal', 'Mandya', 'Mysore', 'Raichur', 'Ramanagara', 'Tumkur', 'Yadgir', 'Ariyalur', 'Coimbatore', 'Dharmapuri', 'Dindigul', 'Erode', 'Karur', 'Krishnagiri', 'Madurai', 'Namakkal', 'Perambalur', 'Salem', 'The Nilgiris', 'Theni', 'Tiruchirappalli', 'Tiruppur', 'Adilabad', 'Hyderabad', 'Karimnagar', 'Khammam', 'Mahbubnagar', 'Medak', 'Nalgonda', 'Nizamabad', 'Ranga Reddy', 'Warangal'],
                      'WPAHR': ['Agar Malwa', 'Alirajpur', 'Barwani', 'Burhanpur', 'Dewas', 'Dhar', 'East Nimar', 'Indore', 'Jhabua', 'Mandsaur', 'Neemuch', 'Ratlam', 'Shajapur', 'Ujjain', 'West Nimar', 'Ahmadnagar', 'Akola', 'Amravati', 'Aurangabad', 'Bid', 'Buldana', 'Dhule', 'Hingoli', 'Jalgaon', 'Jalna', 'Kolhapur', 'Latur', 'Nagpur', 'Nanded', 'Nandurbar', 'Nashik', 'Osmanabad', 'Parbhani', 'Pune', 'Sangli', 'Satara', 'Solapur', 'Wardha', 'Washim', 'Yavatmal', 'Jhalawar'],
                      'ECPHR': ['East Godavari', 'Guntur', 'Krishna', 'Nellore', 'Prakasam', 'Srikakulam', 'Visakhapatnam', 'Vizianagaram', 'West Godavari', 'Baleshwar', 'Bhadrak', 'Cuttack', 'Gajapati', 'Ganjam', 'Jagatsinghapur', 'Jajapur', 'Kendrapara', 'Khordha', 'Nayagarh', 'Puri', 'Karaikal', 'Puducherry', 'Yanam', 'Chennai', 'Cuddalore', 'Kancheepuram', 'Nagappattinam', 'Pudukkottai', 'Ramanathapuram', 'Sivaganga', 'Thanjavur', 'Thiruvallur', 'Thiruvarur', 'Thoothukkudi', 'Tirunelveli', 'Tiruvannamalai', 'Vellore', 'Viluppuram', 'Virudunagar'],
                      'UGPR': ['Agra', 'Aligarh', 'Allahabad', 'Amethi', 'Amroha', 'Auraiya', 'Baghpat', 'Barabanki', 'Bareilly', 'Bijnor', 'Budaun', 'Bulandshahr', 'Etah', 'Etawah', 'Farrukhabad', 'Fatehpur', 'Firozabad', 'Gautam Buddha Nagar', 'Ghaziabad', 'Hapur', 'Hardoi', 'Hathras', 'Kannauj', 'Kanpur Dehat', 'Kanpur Nagar', 'Kasganj', 'Kaushambi', 'Lakhimpur Kheri', 'Lucknow', 'Mainpuri', 'Mathura', 'Meerut', 'Moradabad', 'Muzaffarnagar', 'Pilibhit', 'Pratapgarhup', 'Rae Bareli', 'Rampur', 'Saharanpur', 'Sambhal', 'Shahjahanpur', 'Shamli', 'Sitapur', 'Sultanpur', 'Unnao', 'Hardwar', 'Udham Singh Nagar'],
                      'LGPR': ['Bankura', 'Barddhaman', 'Birbhum', 'Dakshin Dinajpur', 'Haora', 'Hugli', 'Kolkata', 'Maldah', 'Murshidabad', 'Nadia', 'North 24 Parganas', 'Pashchim Medinipur', 'Purba Medinipur', 'South 24 Parganas', 'Uttar Dinajpur'],
                      'MGPR': ['Araria', 'Arwal', 'AurangabadB', 'Banka', 'Begusarai', 'Bhagalpur', 'Bhojpur', 'Buxar', 'Darbhanga', 'Gaya', 'Gopalganj', 'Jamui', 'Jehanabad', 'Kaimur', 'Katihar', 'Khagaria', 'Kishanganj', 'Lakhisarai', 'Madhepura', 'Madhubani', 'Munger', 'Muzaffarpur', 'Nalanda', 'Nawada', 'Pashchim Champaran', 'Patna', 'Purba Champaran', 'Purnia', 'Rohtas', 'Saharsa', 'Samastipur', 'Saran', 'Sheikhpura', 'Sheohar', 'Sitamarhi', 'Siwan', 'Supaul', 'Vaishali', 'Godda', 'Sahibganj', 'Ambedkar Nagar', 'Azamgarh', 'Bahraich', 'Ballia', 'Balrampur', 'Basti', 'Chandauli', 'Deoria', 'Faizabad', 'Ghazipur', 'Gonda', 'Gorakhpur', 'Jaunpur', 'Kushinagar', 'Maharajganj', 'Mau', 'Mirzapur', 'Sant Kabir Nagar', 'Sant Ravi Das Nagar', 'Shravasti', 'Siddharth Nagar', 'Sonbhadra', 'Varanasi'],
                      'TGPR': ['Chandigarh', 'Ambala', 'Bhiwani', 'Faridabad', 'Fatehabad', 'Gurgaon', 'Hisar', 'Jhajjar', 'Jind', 'Kaithal', 'Karnal', 'Kurukshetra', 'Mahendragarh', 'Mewat', 'Palwal', 'Panchkula', 'Panipat', 'Rewari', 'Rohtak', 'Sirsa', 'Sonipat', 'Yamunanagar', 'Delhi', 'Amritsar', 'Barnala', 'Bathinda', 'Faridkot', 'Fatehgarh Sahib', 'Fazilka', 'Firozpur', 'Gurdaspur', 'Hoshiarpur', 'Jalandhar', 'Kapurthala', 'Ludhiana', 'Mansa', 'Moga', 'Muktsar', 'Patiala', 'Sahibzada Ajit Singh Nagar', 'Sangrur', 'Shahid Bhagat Singh Nagar', 'Tarn Taran', 'Ganganagar', 'Hanumangarh'],
                      'WHR': ['Bilaspur', 'Chamba', 'Hamirpur', 'Kangra', 'Kinnaur', 'Kullu', 'Lahul & Spiti', 'Mandi', 'Shimla', 'Sirmaur', 'Solan', 'Una', 'Anantnag', 'Badgam', 'Bandipore', 'Baramulla', 'Doda', 'Ganderbal', 'Jammu', 'Kargil', 'Kathua', 'Kishtwar', 'Kulgam', 'Kupwara', 'Leh (Ladakh)', 'Poonch', 'Pulwama', 'Rajouri', 'Ramban', 'Reasi', 'Samba', 'Shupiyan', 'Srinagar', 'Udhampur', 'Pathankot', 'Rupnagar', 'Almora', 'Bageshwar', 'Chamoli', 'Champawat', 'Dehradun', 'Garhwal', 'Nainital', 'Pithoragarh', 'Rudraprayag', 'Tehri Garhwal', 'Uttarkashi'],
                      'EHR': ['Anjaw', 'Changlang', 'Dibang Valley', 'East Kameng', 'East Siang', 'Kurung Kumey', 'Lohit', 'Longding', 'Lower Dibang Valley', 'Lower Subansiri', 'Namsai', 'Papum Pare', 'Tawang', 'Tirap', 'Upper Siang', 'Upper Subansiri', 'West Kameng', 'West Siang', 'Baksa', 'Barpeta', 'Bongaigaon', 'Cachar', 'Chirang', 'Darrang', 'Dhemaji', 'Dhubri', 'Dibrugarh', 'Dima Hasao', 'Goalpara', 'Golaghat', 'Hailakandi', 'Jorhat', 'Kamrup Metropolitan', 'Kamrup', 'Karbi Anglong', 'Karimganj', 'Kokrajhar', 'Lakhimpur', 'Morigaon', 'Nagaon', 'Nalbari', 'Sivasagar', 'Sonitpur', 'Tinsukia', 'Udalguri', 'Bishnupur', 'Chandel', 'Churachandpur', 'Imphal East', 'Imphal West', 'Senapati', 'Tamenglong', 'Thoubal', 'Ukhrul', 'East Garo Hills', 'East Khasi Hills', 'Jaintia Hills', 'North Garo Hills', 'Ri Bhoi', 'South Garo Hills', 'South West Garo Hills', 'South West Khasi Hills', 'West Garo Hills', 'West Khasi Hills', 'Aizawl', 'Champhai', 'Kolasib', 'Lawangtlai', 'Lunglei', 'Mamit', 'Saiha', 'Serchhip', 'Dimapur', 'Kiphire', 'Kohima', 'Longleng', 'Mokokchung', 'Mon', 'Peren', 'Phek', 'Tuensang', 'Wokha', 'Zunheboto', 'East Sikkim', 'North Sikkim', 'South Sikkim', 'West Sikkim', 'Dhalai', 'Gomati', 'Khowai', 'North Tripura', 'Sipahijala', 'South Tripura', 'Unokoti', 'West Tripura', 'Alipurduar', 'Darjiling', 'Jalpaiguri', 'Koch Bihar']
};

acz = acronym;

var dist_list = dist_list_dict[acz];
var india_district = ee.FeatureCollection('projects/ee-indiasat/assets/india_district_boundaries');
var aoi = india_district.filter(ee.Filter.eq('Name', dist_list[0])).geometry();

for (var i=1; i<dist_list.length; i++) {
  var new_aoi = india_district.filter(ee.Filter.eq('Name', dist_list[i])).geometry();
  aoi = aoi.union(new_aoi);
}

for (var k=0; k<res_list.length; k++) {
  var res = res_list[k];

  var fc = ee.FeatureCollection(project_path + '/ch_' + year + res  + acronym);
  var bands = ['rh50_class', 'rh75_class', 'rh98_class', 'ch_class'];

  var district_img = ee.Image(0);

  for (var i=0; i<4; i++) {

    var image = fc.reduceToImage({
      // List of properties to convert to bands
      properties: [bands[i]],

      // Reducer function (optional, default is 'first')
      reducer: ee.Reducer.first() // Chooses the value from the first feature for each pixel
    });

    image = image.rename([bands[i]]);
    // print(image);

    district_img = district_img.addBands(image);
  }

  district_img = district_img.reproject('EPSG:4326', null, 25).clip(aoi).select(bands);
  print(district_img);

  Export.image.toAsset({
    image: district_img,
    description: 'fc_to_image_ch_' + year + '_' + acronym,
    assetId: outpath_path + '/ch_'+ year + '/ch_' + year + res + acronym,
    region: aoi,
    scale: 25,
    crs: 'EPSG:4326',
    maxPixels: 10000000000
  });
}
