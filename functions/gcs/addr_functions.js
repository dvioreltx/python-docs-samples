function scoreMatchFor(str1, str2, type, city) {

  if (str1 == undefined || str1 == null || str1 == '' || str2 == undefined || str2 == null || str2 == '') { return 0;}

  str1 = cleanStr(str1, type);
  str2 = cleanStr(str2, type);

  if (str1 == undefined || str1 == null || str1 == '' || str2 == undefined || str2 == null || str2 == '') { return 0;}

  str1 = str1.trim();
  str2 = str2.trim();

  // exact match?
  if (str1 == str2) {
    return 1.2;

    // guess not
  } else {

    //var n = (type == 'addr' || type == 'street') ? 1 : 2;
    n = 2;

    var arr1 = filterArray(str1.split(' '), n);
    var arr2 = filterArray(str2.split(' '), n);

    if (arr1.length == 0 || arr2.length == 0) {
      return 0;
    }

    var pct1 = pct_match(arr1,arr2, type, city);
    return (pct1 >= 1) ? pct1 :  Math.max(pct1, pct_match(arr2, arr1, type, city));
  }
}

///////////////////////////////////////////////////////////////////////
function cleanStreetNum(str) {
  if (str == undefined || str == null || str == '') { return str; }

  // two leading numbers in a row, throw out first numbers
  // e.g., 1 234 N Main St = 234 N Main St
  var regex = /^[0-9]+ [0-9]+/g;

   var parts = str.split(' ');

  if( str.match(regex) === null ) {
  } else {
    parts.shift();
    str = parts.join(' ');
  }

  parts = str.split(' ');

  var street_in  = (str.match(/ /)) ? parts[0] : str;
  var street_out = street_in;

  // only parse numerical addrs
  if (street_in.match(/[a-z]/)) {
    return str;
  }

  // if given range of streets, take middle street number
  if (street_in.match('-')) {
    var a = parts[0].split('-');
    var x = a[0];
    var y = a[1];
    street_out = Math.round((+x + +y)/2);
  }

  return str.replace(street_in, street_out);
}

///////////////////////////////////////////////////////////////////////
function cleanStr(str, type) {

  if (str == undefined || str == null || str == '') { return str; }

  str = str.replace(/\s\s+/g, ' ');

  if (type == 'addr' || type == 'street') {
    str = cleanStreetNum(str);
  }

  if (type == 'chain') {
    str = str.replace(/The Limited Too/,                                 'Justice');
    str = str.replace(/Super America/,                                   'Speedway');
    str = str.replace(/The Pantry/,                                      'Kangaroo Express');
    str = str.replace(/Ross Stores|Ross Dress for Less/,                 'RossDressforLess');
    str = str.replace(/J.Crew|J. Crew/,                                  'JCrew');
    str = str.replace(/J.Jill|J. Jill/,                                  'JJill');
    str = str.replace(/T.J. Maxx|T.J.Maxx/,                              'TJMaxx');
    str = str.replace(/Dress Barn/,                                      'Dressbarn');
    str = str.replace(/John Deere.*/,                                    'JohnDeere');
    str = str.replace(/PNC Bank|PNC Financial Services/,                 'PNCFinancialServices');
    str = str.replace(/Goodwill.*/,                                      'GoodwillIndustries');
    str = str.replace(/BB&T.*/,                                          'BBandT');
    str = str.replace(/Shell Food Mart|Shell Oil/,                       'ShellOil');
    str = str.replace(/Master Cuts/,                                     'MasterCuts');
    str = str.replace(/Holiday Station|Holiday Stationstores/,           'HolidayStation');
    str = str.replace(/International House Of Pancakes|IHOP/,            'IHOPancakes');
    str = str.replace(/Country Inn & Suites.*|Country Inns & Suites.*/,  'CountryInnSuites');
    str = str.replace(/Compass Bank|BBVA Compass Bancshares/,            'BBVACompassBancshares');
    str = str.replace(/Comfort Inn|Comfort Suites/,                      'ComfortInnSuites');
    str = str.replace(/Ez Pawn/,                                         'EZPawn');
    str = str.replace(/Hy-Vee/,                                          'HyVee');
    str = str.replace(/Regis Salons|Regis Hairstylists/,                 'RegisHairstylists');
    str = str.replace(/Embassy Suites By Hilton|Embassy Suites Hotels/,  'EmbassySuites');
    str = str.replace(/Rally's Drive-In Restaurants|Rallys Hamburgers/,  'RallysHamburgers');
    str = str.replace(/Radio Shack/,                                     'RadioShack');
    str = str.replace(/99 Cents Only Stores|99 Cents Only/,              '99CentsOnly');
    str = str.replace(/A.ropostale/,                                     'Aeropostale');
    str = str.replace(/Caf./,                                            'Cafe');
    str = str.replace(/Gonz.lez/,                                        'Gonzalez');
    str = str.replace(/Einstein Bros Bagels|Einstein Brothers/,          'EinsteinBrothers');
    str = str.replace(/Walmart Supercenter/,                             'Walmart');
    str = str.replace(/Togo's Eateries|Togo's Eatery/,                   'TogosEatery');
    str = str.replace(/Dippin' Dots/,                                    'DippinDots');
    str = str.replace(/Donatos Pizza|Donatos Pizzeria/,                  'Donatos');
    str = str.replace(/Fidelity Express|Fidelity Bank/,                  'FidelityExpress');
    str = str.replace(/Town Pump/,                                       'TownPump');
    str = str.replace(/Taco Time/,                                       'TacoTime');
    str = str.replace(/Dress Barn/,                                      'Dressbarn');
    str = str.replace(/Pacific Sunwear Of California/,                   'PacSun');
    str = str.replace(/Quick Cash Inc|Quik Cash/,                        'QuikCash');
    str = str.replace(/Big O Tires/,                                     'BigOTires');
    str = str.replace(/Super 8/,                                         'Super8');
    str = str.replace(/H&M.*/,                                           'HandM');
    str = str.replace(/Eat.*Park/,                                       'EatNPark');
    str = str.replace(/Back Yard Burgers|Backyard Burgers/,              'BackyardBurgers');
    str = str.replace(/Verizon payment center|Verizon Wireless/,         'Verizon');
    str = str.replace(/Reebok Store/,                                    'Reebok');
    str = str.replace(/Levi Strauss.*|Levi'S.*/,                         'LeviStrauss');
    str = str.replace(/Ninety Nine Restaurant.*/,                        'NinetyNineRestaurants');
    str = str.replace(/Rouses Markets|Rouses Supermarkets/,              'RousesMarkets');
    str = str.replace(/Rita.*Ice/,                                       'RitasWaterIce');
    str = str.replace(/Wingate.*Wynd.*/,                                 'WingateWyndham');
    str = str.replace(/Enterprise.*Sales|Enterprise Rent.*/,             'EnterpriseRentACar');
    str = str.replace(/.*Bank.*America.*/,                               'BankOfAmerica');
    str = str.replace(/Claire[']?s.*/,                                   'ClairesBoutiques');
    str = str.replace(/United States Post.*/,                            'USPostOffice');
    str = str.replace(/FedEx.*/,                                         'FedEx');
    str = str.replace(/Starbuck.*/,                                      'Starbucks');
    str = str.replace(/Radio Shack/,                                     'RadioShack');
    str = str.replace(/Kentucky Fried Chicken|KFC/,                      'KentuckyFriedChicken');
    str = str.replace(/Ninety Nine Restaurant.*/,                        'NinetyNineRestaurant');
    str = str.replace(/Budget.*Rent.*/,                                  'BudgetCarTruckRental');
    str = str.replace(/America[']?s Best.*Inn.*/,                        'AmericasBestValueInn');
    str = str.replace(/Rent-A-Car|Rent A Car/,                           'RentACar');
    str = str.replace(/National Association.*Young Children.*|.*Children.*Place/, 'ChildrensPlace');
    str = str.replace(/People's United Bank|Peoples Bank/, 'PeoplesBank');
  }

  str = str.replace(/'/g, '').toLowerCase();
  str = str.replace(/[/]/g, ' ');
  str = str.replace(/[&_-]/g, ' ');

  // remove store and suite #s (e.g., items with #1234...)
  str = str.replace(/([ ]?[#] *[0-9]+)([ ]|$)/g,  '$2');

  // remove other special characters
  str = str.replace(/[^a-z0-9 ]/g, '');

  str = str.replace(/ grv([ ]|$)/g,    ' grove$1');
  str = str.replace(/ hgts([ ]|$)/g,   ' heights$1');
  str = str.replace(/ twp([ ]|$)/g,    ' township$1');
  str = str.replace(/ mkt([ ]|$)/g,    ' market$1');
  str = str.replace(/ supcnt([ ]|$)/g, ' supercenter$1');

  str = str.replace(/wal mart/g,       'walmart');
  str = str.replace(/k mart/g,         'kmart');
  str = str.replace(/re max/g,         'remax');
  str = str.replace(/^at t([ ]|$)/g,   'atandt$1');

  if (type == 'addr' || type == 'street') {

    // split main45 into main 45
    str = str.replace(/(^|[ ])([a-z]+)([0-9]+)/g, '$1$2 $3');

    // split 45main into 45 main, but keep 1st, 23rd, 11th and 42nd as is
    str = str.replace(/(^|[ ])([0-9]+)(?!(st|rd|nd|th))([a-z]+)/g, '$1$2 $4');

    str = str.replace(/ n([ ]|$)/g,         ' north$1');
    str = str.replace(/ s([ ]|$)/g,         ' south$1');
    str = str.replace(/ e([ ]|$)/g,         ' east$1');
    str = str.replace(/ w([ ]|$)/g,         ' west$1');
    str = str.replace(/ ste([ ]*)(.*)/g,    ' suite $2');
    str = str.replace(/ rd([ ]|$)/g,        ' road$1');
    str = str.replace(/ st(r?)([ ]|$)/g,    ' street$2');
    str = str.replace(/ av(e?)([ ]|$)/g,    ' avenue$2');
    str = str.replace(/ a$/g,               ' avenue');
    str = str.replace(/ rt(e?)([ ]|$)/g,    ' route$2');
    str = str.replace(/ dr([ ]|$)/g,        ' drive$1');
    str = str.replace(/ pl([ ]|$)/g,        ' place$1');
    str = str.replace(/ blv(d)?([ ]|$)/g,   ' boulevard$2');
    str = str.replace(/ sq([ ]|$)/g,        ' square$1');
    str = str.replace(/ fl([ ]|$)/g,        ' floor$1');
    str = str.replace(/ cir([ ]|$)/g,       ' circle$1');
    str = str.replace(/ trl([ ]|$)/g,       ' trail$1');
    str = str.replace(/ ln([ ]|$)/g,        ' lane$1');
    str = str.replace(/ hwy/g,              ' highway');
    str = str.replace(/ xing/g,             ' crossing');
    str = str.replace(/ fwy/g,              ' freeway');
    str = str.replace(/ expy/g,             ' expressway');
    str = str.replace(/ ctr/g,              ' center');
    str = str.replace(/ bldg/g,             ' building');
    str = str.replace(/ tpke/g,             ' turnpike');
    str = str.replace(/ pkwy/g,             ' parkway');
    str = str.replace(/ plz/g,              ' plaza');
    str = str.replace(/ hts/g,              ' heights');
    str = str.replace(/ ct/g,               ' court');
  }

  return str.trim();
}

///////////////////////////////////////////////////////////////////////
function pct_match(arr1, arr2, type, city) {

  if (arr1.length == 0 || arr2.length == 0) { return 0; }

  var arr1words     = arr1.length;
  var score         = 0;
  var hasaStreetNum = 0;

  var directionalElements   = 'north|south|east|west';

  var housingElements       = directionalElements
  + '|suite|road|street|avenue|route|drive|place|boulevard'
  + '|square|highway|crossing|expressway|building|turnpike'
  + '|parkway|plaza|heights|unit|floor|court|trail|circle|freeway|lane';

  var storeElements         = 'store|stores|depot|the|and|bank|inc|tavern|grill|pizza'
  + '|house|food|america|fitness|cash|chicken|wireless|cleaner|hair|salon|international|inn|'
  + cleanStr(city, 'city');

  var hasDirectionalElement = 0;
  var directionalIndex      = 0;
  var nonHousingMatch       = 0;
  var nonHousingElements    = 0;
  var streetNumFactor       = 1;
  var storeElementMatches   = 0;
  var totalMatches          = 0;


  // check street numbers (anything w/in 3 blocks is good enough)
  if(arr1[0].match(/[0-9]+/) == arr1[0] && arr2[0].match(/[0-9]+/) == arr2[0]) {
    hasaStreetNum++;

    var street1     = arr1[0];
    var street2     = arr2[0];
    var housesAway  = Math.abs((+street1 - +street2)/2);
    if (city == 'sic_code' || city == 'relaxed' || city == 'lax') {
      // for sic_code matches, allow distance to be further away than normal
      streetNumFactor = Math.max(.05, Math.max(0, (1000 - housesAway)/1000));
    } else {
      streetNumFactor = Math.max(.05, Math.max(0, (100 - housesAway)/100));
    }
  }

  var c = 0;
  // check each word in str1 in str2
  arr1.forEach(function(target) {

    if (target == arr1[0] && hasaStreetNum > 0) {
      // skip

    } else {

      // remove s from end of words
      //target = target.replace(/(.*)s$/, '$1').trim();

      var isaHousingItem     = 0;
      var isaDirectionalItem = 0;

      // anything immediately following n/s/e/w is the street name
      if (directionalIndex > 0 && c == directionalIndex+1) {
        nonHousingElements++;
      } else if (target.match(housingElements) == target) {
        isaHousingItem++;
      }  else {
        nonHousingElements++;
      }

      // check for n/s/e/w specifically
      if (target.match(directionalElements) == target) {
        isaDirectionalItem++;
        hasDirectionalElement++;
        if(directionalIndex == 0) { directionalIndex = c };
      }

      var matched = 0;

      arr2.forEach(function(source) {

        if (source == arr2[0] && hasaStreetNum > 0) {
          // skip

        } else {
          //source = source.replace(/(.*)s$/, '$1').trim();

          // houston, we have a match
          if (source.match(target) && matched == 0) {
            totalMatches++;
            matched++;
            var a = target.length;
            var b = source.length;

            if (isaHousingItem == 0 || (isaDirectionalItem > 0 && hasDirectionalElement > 1)) {
              nonHousingMatch++;
            }

            if (type == 'chain' && source.match(storeElements) == source) {
              storeElementMatches++;
            }

            score += a/b; // pct overlap
          }
        }

      }); // end arr2 loop
    }
    c++;
  }); // end arr1 loop

  var pct = 0;
  if (hasaStreetNum > 0 && type == 'addr') {
    if (streetNumFactor == 1  && nonHousingMatch > 0) {
      pct = (score+1+nonHousingMatch)/arr1words;
    } else {
      pct = streetNumFactor * (score / (arr1words - 1));
    }
  } else {
    pct =  (score/arr1words);
  }

  // multiple score by percent of non-housing elements matched
  var nonHousingPct = 1;
  if (type == 'addr') {
    nonHousingPct = (nonHousingElements > 0) ? nonHousingMatch/nonHousingElements : .95;
  }

  pct = (Math.min(1, pct * nonHousingPct)).toFixed(3);

  // if we *only* match on store, depot, etc then discard the match
  if (type == 'chain' && (storeElementMatches == totalMatches) ) {
    pct = 0;
  }

  return pct;
}

///////////////////////////////////////////////////////////////////////
function filterArray(array, n) {

  if (array.length == 0) { return array; }

  return array.filter(function(value, index, arr){
    if(value.match(/[a-z]/)) {
      return value.length > n ;
    } else {
      // remove single digit numbers, but keep 10+
      return value.length > 1;
    }
  });
}

///////////////////////////////////////////////////////////////////////
function zip5(zip) {
  zip = zip.split(/-/)[0];
  if(zip.length == 3) {
    zip = '00' + zip;
  } else if (zip.length == 4) {
    zip = '0' + zip;
  }
  return zip;
}
