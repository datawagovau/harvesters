import ckanapi
import csv
from datetime import datetime
import json
import requests
import os
from owslib.wms import WebMapService
from owslib.wfs import WebFeatureService
from pyproj import Proj, transform
import re
from slugify import slugify


#-------------------------------------------------------------------------------------#
# SLIP Classic
#-------------------------------------------------------------------------------------#

def make_slip_wfs_name(dataset_name):
    """
    Extract the SLIP WFS layer name from a dataset name.
    
    >>> make_slip_wfs_name('LGATE-001')
    'slip:LGATE-001'
    """
    return "slip:{0}".format(dataset_name.upper())


def make_dataset_name(slip_wfs_name):
    """Extract the dataset name from a SLIP WFS layer name
    
    >>> make_dataset_name('slip:LGATE-001')
    'LGATE-001'
    """
    return slip_wfs_name.split(":")[1]


def parse_name(text, debug=False):
    """Split a string of LAYER NAME (OPTIONAL EXTRAS) (LAYER ID) (OPTIONAL LAST UPDATED)
    into Layer name (optional extras), layer ID and date last updated
    
    Arguments:
        text (String) text, e.g.:
            Ramsar Sites (Dpaw-037) (28-10-2014 11:11:15)
            Hydrographic Catchments - Basins (Dow-013) (03-11-2008 15:07:44)
            Hydrographic Catchments - Basins (Dow-013)
            Misc Transport (Point) (Lgate-037) (18-10-2012 16:54:00)
    Returns:
        A tuple of (layer title, id, published date)
    
    Examples:
    
    >>> parse_name("Hydrographic Catchments - Basins (Dow-013) (03-11-2008 15:07:44)")
    INPUT
      text: Hydrographic Catchments - Basins (Dow-013) (03-11-2008 15:07:44)
      Testing whether last parenthesis is a date, input: 15:07:44)
      Testing whether 03-11-2008 15:07:44 parses as a valid date...
      ...success, got 2008-11-03T15:07:44
    OUTPUT
      title: Hydrographic Catchments - Basins
      name: dow-013
      date: 2008-11-03T15:07:44
      
    >>> parse_name("Misc Transport (Point) (Lgate-037) (18-10-2012 16:54:00)", debug=True)
    INPUT
      text: Misc Transport (Point) (Lgate-037) (18-10-2012 16:54:00)
      Testing whether last parenthesis is a date, input: 16:54:00)
      Testing whether 18-10-2012 16:54:00 parses as a valid date...
      ...success, got 2012-10-18T16:54:00
    OUTPUT
      title: Misc Transport (Point)
      name: lgate-037
      date: 2012-10-18T16:54:00

    >>> parse_name("Hydrographic Catchments - Basins (Dow-013)", debug=True)
    INPUT
      text: Hydrographic Catchments - Basins (Dow-013)
      Testing whether last parenthesis is a date, input: ['Hydrographic', 'Catchments', '-', 'Basins', '(Dow-013)']
      Last text part starts with parenthesis, so it's not a date: (Dow-013)
      No valid date found, inserting current datetime as replacement
    OUTPUT
      title: Hydrographic Catchments - Basins
      name: dow-013
      date: 2015-10-05T13:41:48
      
    >>> parse_name("Overview Rivers(LGATE-053) (14-05-2008 17:59:05)", debug=True)
    INPUT
      text: Overview Rivers(LGATE-053) (14-05-2008 17:59:05)
      Testing whether last parenthesis is a date, input: ['Overview', 'River', '(LGATE-053)', '(14-05-2008', '17:59:05)']
      Testing whether 14-05-2008 17:59:05 parses as a valid date...
      ...success, got 2008-05-14T17:59:05
    OUTPUT
      title: Overview River
      name: lgate-053
      date: 2008-05-14T17:59:05
      
    >>> parse_name("Graticule (REF-001)", debug=True)
    INPUT
      text: Graticule (REF-001)
      Testing whether last parenthesis is a date, input: ['Graticule', '(REF-001)']
      Last text part starts with parenthesis, so it's not a date: (REF-001)
      No valid date found, inserting current datetime as replacement
    OUTPUT
      title: Graticule
      name: ref-001
      date: 2015-10-05T13:41:03
      
    >>> parse_name("Virtual Mosaic", debug=True)
    INPUT
      text: Virtual Mosaic
      Testing whether last parenthesis is a date, input: Mosaic
      Testing whether Virtual Mosaic parses as a valid date...
      ...failure. Using current datetime instead.
      No valid date found, inserting current datetime as replacement
      No name slug found
    OUTPUT
      title: Virtual Mosaic
      name: None
      date: 2015-10-08T17:14:42
    """
    if debug:
        print("INPUT\n  text: {0}".format(text.encode('utf-8')))

    min_length = 4 # title, name, date, time
    chop_off = 3 # chop off name, date, time to retain title
    date_missing = False
    set_dummy_date = False
    
    # Assert that there's whitespace before opening parentheses
    # Looking at you, "Overview Rivers(LGATE-053) (14-05-2008 17:59:05)":
    text = re.sub(r"[a-z]\(", u" (", text)
    
    p = text.encode('utf-8').split()
    
    if debug:
        print("  Testing whether last parenthesis is a date, input: {0}".format(str(p[-1])))
    
    # If last part starts with a parenthesis, it's not the date, but the name
    if p[-1].startswith("("):
        if debug:
            print("  Last text part starts with parenthesis, so it's not a date: {0}".format(p[-1]))
        chop_off = 1
        date_missing = True
        set_dummy_date = True
    
    if not date_missing:
        d = "{0} {1}".format(p[-2].replace("(", ""), p[-1].replace(")", ""))
        try:
            if debug:
                print("  Testing whether {0} parses as a valid date...".format(d))
            dt = datetime.strptime(d, "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S")
            if debug:
                print("  ...success, got {0}".format(dt))
        except ValueError:
            if debug:
                print("  ...failure. Using current datetime instead.")
            set_dummy_date = True
    
    if set_dummy_date:
        if debug:
            print("  No valid date found, inserting current datetime as replacement")
        dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    if p[-1].endswith(")"):
        n = p[-chop_off].replace("(", "").replace(")","").lower()
    else:
        if debug:
            print("  No name slug found")
        chop_off = 0
        n = None
            
    t = " ".join(p[0:len(p)-chop_off])
    if debug:
        print("OUTPUT\n  title: {0}\n  name: {1}\n  date: {2}".format(t, n, dt))
    return (t, n, dt)


def bboxWGSs84_to_gjMP(bbox):
    """Return a WMS layer's layer.
    
    Arguments:
        bbox (owslib.wms.ContentMetadata.boundingBoxWGS84): A WGS84 bbox 
            from an owslib WMS layer content
    
    Returns:
        dict A GeoJSON MultiPolygon Geometry string in WGS84
    """
    e, s, w, n = bbox
    return json.dumps({"type": "MultiPolygon", 
                       "coordinates": [[[[e,n],[e,s],[w,s],[w,n]]]]})


def wxs_to_dict(layer, wxs_url, org_dict, group_dict, pdf_dict, 
                fallback_org_id=None, res_format="WMS", debug=False):
    '''Convert a WMS layer into a dict of a datawagovau-schema CKAN package.

    This function is highly customised to harvesting 
    Landgate's SLIP WMS into data.wa.gov.au.
    
    Assumption: All WMS layers have parent layers from which we've 
    created groups, and group_dict is the result of ckanapi's
    `group_list` or the sum of all results of `group_create/update`.
    
    
    m = wmsP.contents["DAA-001"]
    m.id, m.title, m.abstract, m.boundingBoxWGS84, m.crsOptions, m.keywords, m.parent.title
    ('DAA-001',
     'Aboriginal Heritage Places (DAA-001) (14-10-2015 21:40:58)',
     None,
     (112.892, -35.0874, 129.925, -13.7403),
     ['EPSG:4283',
      'EPSG:20352',
      'EPSG:20351',
      'EPSG:20350',
      'EPSG:20349',
      'EPSG:4326',
      'EPSG:3857',
      'EPSG:900913',
      'EPSG:28349',
      'EPSG:3785',
      'EPSG:102100',
      'EPSG:4203',
      'EPSG:102113',
      'EPSG:28352',
      'EPSG:28351',
      'EPSG:28350'],
     [],
     'Cultural, Society and Demography')
     
     
     
    make_slip_wfs_name("DAA-001")
    l = wfsP.contents["slip:DAA-001"]
    l.id, l.title, l.abstract, l.boundingBoxWGS84, l.crsOptions, l.keywords, l.verbOptions
    ('slip:DAA-001',
     'Aboriginal Heritage Places (DAA-001)',
     'MF:2000',
     (112.891525268555, -35.0873985290527, 129.925109863281, -13.740309715271),
     [urn:ogc:def:crs:EPSG::4326],
     ['daa_001 DAA'],
     ['{http://www.opengis.net/wfs}Query',
      '{http://www.opengis.net/wfs}Insert',
      '{http://www.opengis.net/wfs}Update',
      '{http://www.opengis.net/wfs}Delete',
      '{http://www.opengis.net/wfs}Lock'])
    

    Arguments:
        layer (owslib.wms.ContentMetadata): A WMS object content layer
        wms_url (String): The resource URL for WMS layers
        wfs (owslib.wfs.WebFeatureService): An owslib WFS object
        wfs_url (String): The resource URL for WFS layers
        org_dict (dict): The output of ckanapi's organsation_list
        group_dict (dict): The output of ckanapi's group_list
        pdf_dict (dict): A wmslayer-named dict of resource metadata of PDFs
        fallback_org_id (String): The CKAN ID of the fallback owner organisation
        res_format (String): The resource format (WMS, WFS, WPS, WCS), default: WMS
        debug (Boolean): Debug noise level
        
    Returns:
        dict: A dictionary ready for ckanapi's package_update
    
    @example 
    >>> from owslib.wms import WebMapService
    >>> import ckanapi
    >>> wms = WebMapService(WMS_URL, version='1.1.1')
    >>> ckan = ckanapi.RemoteCKAN("http://landgate.alpha.data.wa.gov.au/", apikey=APIKEY)
    >>> ckan = ckanapi.RemoteCKAN(CKAN["wwba"]["url"], apikey=CKAN["wwba"]["key"])
    >>> pdf_dict = get_pdf_dict("data-dictionaries.csv")
    >>> org_dict = get_org_dict("organisations.csv")
    >>> orgs = upsert_orgs(org_dict, ckan, debug=False)
    >>> layer_dict = get_layer_dict(wms_layer, WMS_URL, wfs, WFS_URL, orgs, groups, pdf_dict)
    '''
    try:
        n = layer.name
        # add more checks to pick up the copyright layer
    except:
        #print("[wms_to_dict] Yuck, that was not a WMS layer *spits*")
        #return(None)
        n = make_dataset_name(layer.id)

    d = dict()
    
    (ds_title, ds_name, date_pub) = parse_name(layer.title, debug)
    if ds_name is None:
        print("[wms_to_dict] No dataset name found, skipping")
        return(None)
    ds_NAME = ds_name.upper()
    
    # Theme, Keywords and Group (only from WMS parent layer)
    d["tag_string"] = ["SLIP Classic", "Harvested"]
    try:
        p = layer.parent.title
        
        d["theme"] = p
        
        grp_dict = group_dict.get(p, None)
        
        grp_id = grp_dict.get("id", None)
        if grp_id:
            grp = dict()
            grp["id"] = grp_id
            d["groups"] = [grp,]
        
        grp_name = grp_dict.get("name", None)
        if grp_name:
            d["tag_string"].append(grp_name)
        
    except:
        if debug:
            print("[wxs_to_dict] Skipping Theme, Keywords, Group - "+\
                  "no parent layer found for {0} layer {1}".format(
                    res_format, ds_name))

    
    org_name = ds_name.split("-")[0]
    owner_org_dict = org_dict.get(org_name, None)
    owner_org_id = owner_org_dict.get("id") if owner_org_dict and owner_org_dict.has_key("id") else fallback_org_id 
    owner_org_title = owner_org_dict.get("title") if owner_org_dict and owner_org_dict.has_key("title") else ""
    extras = owner_org_dict.get("extras") if owner_org_dict and owner_org_dict.has_key("extras") else ""
    if extras:
        owner_org_contact = [x["value"] for x in extras if x["key"]=="Contact"][0]
        owner_org_jurisdiction = [x["value"] for x in extras if x["key"]=="Jurisdiction"][0]
    else:
        owner_org_contact = ""
        owner_org_jurisdiction = "Western Australia (default)"
    
    slip_description = u"when prompted, use your [SLIP](https://www2.landgate.wa.gov.au/"+\
    u"web/guest/how-to-access-slip-services) "+\
    u"username and password to preview the resource below "+\
    u"or open the resource URL in a GIS application (e.g. QGIS or ArcGIS) as layer _{0}_.".format(ds_NAME)

    d["name"] = slugify(ds_title)
    d["title"] = ds_title
    #d["doi"] = ""
    #d["citation"] = ""
    d["notes"] = u"The dataset _{0}_ has".format(ds_NAME) +\
    " been sourced from Landgate's " +\
    u"Shared Location Information Platform (SLIP) - the home for Western" +\
    u" Australian government geospatial data.\n\nMany of the datasets in" +\
    u" SLIP are free and publicly available to users who simply " +\
    u"[sign up for a SLIP account](https://www2.landgate.wa.gov.au/web/guest" +\
    u"/request-registration-type).\n\nFind out more about SLIP at " +\
    u"[http://slip.landgate.wa.gov.au/](http://slip.landgate.wa.gov.au/)."
    
    d["owner_org"] =  owner_org_id
    
    d["data_portal"] = "http://slip.landgate.wa.gov.au/"
    d["data_homepage"] = ""
    d["license_id"] = "cc-by-sa"
    d["author"] = owner_org_title
    d["author_email"] = owner_org_contact
    d["maintainer_email"] = "customerservice@landgate.wa.gov.au"
    d["maintainer"] = "Landgate"
    d["private"] = False
    d["spatial"] = bboxWGSs84_to_gjMP(layer.boundingBoxWGS84)
    d["published_on"] = date_pub
    d["last_updated_on"] = date_pub
    d["update_frequency"] = "frequent"
    #d["data_temporal_extent_begin"] = ""
    #d["data_temporal_extent_end"] = ""

    resource_list = []
    
    # Attach Data Dict PDF as resource if available
    pdf_url = pdf_dict.get(ds_name, None)
    if pdf_url:
        if debug:
            print("  Found PDF resource")
        r = dict()
        r["description"] = "Data Dictionary for {0}".format(ds_NAME)
        r["format"] = "PDF"
        r["name"] = "Data dictionary and dataset metadata"
        r["url"] = pdf_url
        d["data_homepage"] = pdf_url
        resource_list.append(r)
        
    # Attach WMS/WFS endpoint as resource
    r = dict()
    r["description"] = slip_description
    r["format"] = res_format.lower()
    r["name"] = "{0} ({1}) {2}".format(ds_title, ds_NAME, res_format.upper())
    r["url"] = wxs_url
    r["{0}_layer".format(res_format.lower())] = ds_NAME
    resource_list.append(r)
    
    d["resources"] = resource_list
    
    if debug:
        print("[wxs_to_dict] Returning package dict \n{0}".format(str(d)))

    return d



def gs28_to_ckan(layer, wxs_url, ckan, 
                 fallback_org_id=None, res_format="WMS", debug=False):
    """Convert a GeoServer 2.8 WMS layer into a dict of a datawagovau-schema CKAN package.
    
    This function is tailored towards kmi.dpaw.wa.gov.au's implementation.
    """

    d = dict()
    
    org_name = layer.name.split(":")[0]
    try:
        owner_org = ckan.action.organization_show(id=org_name)
        owner_org_id = owner_org["id"]
    except NotFound:
        owner_org_id = fallback_org_id

    d["name"] = slugify(layer.name)
    d["title"] = layer.title
    #d["doi"] = ""
    #d["citation"] = ""
    d["notes"] = layer.abstract or ""
    d["owner_org"] = owner_org_id
    d["tag_string"] = ["Knowledge Management Initiative", "KMI", "Harvested"]
    d["data_portal"] = "http://kmi.dpaw.wa.gov.au/geoserver/web/"
    d["data_homepage"] = ""
    d["license_id"] = "cc-by-sa"
    d["author"] = layer.parent.title
    d["author_email"] = ""
    d["maintainer_email"] = "marinedatarequests@dpaw.wa.gov.au"
    d["maintainer"] = "Marine Data Manager"
    d["private"] = False
    d["spatial"] = bboxWGSs84_to_gjMP(layer.boundingBoxWGS84)
    #d["published_on"] = None
    #d["last_updated_on"] = None
    d["update_frequency"] = "frequent"
    #d["data_temporal_extent_begin"] = ""
    #d["data_temporal_extent_end"] = ""

    resource_list = []
        
    # Attach WMS/WFS endpoint as resource
    r = dict()
    r["description"] = layer.parent.title
    r["format"] = res_format.lower()
    r["name"] = "{0} {1}".format(layer.title, res_format.upper())
    r["url"] = wxs_url
    r["{0}_layer".format(res_format.lower())] = layer.name
    resource_list.append(r)
    
    d["resources"] = resource_list
    
    if debug:
        print("[wxs_to_dict] Returning package dict \n{0}".format(str(d)))

    return d

def get_layer_dict_gs28(wxs, wxs_url, ckanapi, 
                        fallback_org_name='dpaw', res_format="WMS", 
                        debug=False):
    """Return a list of CKAN API package_show-compatible dicts
    
    Arguments:
    
        wxs A wxsclient loaded from a WXS enpoint
        wxs_url The WXS endpoint URL to use as dataset resource URL
        ckanapi A ckanapi instance with at least read permission
        org_dict A dict of CKAN org names and ids
        pdf_dict A dict of dataset names and corresponding PDF URLs
        debug Debug noise
        fallback_org_name The fallback CKAN org name , default:'lgate'    
    
    Returns:
        A list of CKAN API package_show-compatible dicts
    """
    foid = ckanapi.action.organization_show(id=fallback_org_name)["id"]
    return [gs28_to_ckan(wxs.contents[layername], wxs_url, ckanapi, 
                        fallback_org_id=foid, res_format=res_format,
                        debug=debug) for layername in wxs.contents]


def add_resource_to_list(resourcedict_list, resource_dict, debug=False):
    """Add a single resource_dict to a resourcedict_list if URL is unique
    
    Arguments:
        resourcedict_list (List of dicts): package_show(id="xxx")["resources"]
        resource_dict (dict): One resource dict
        debug (Boolean): Debug noise level
        
    Returns:
        List of resource dicts
    """
    if not resource_dict["url"] in [r["url"] for r in resourcedict_list]:
        if debug:
            print("[add_resource_to_list] New resource added with unique URL {0}".format(
                    resource_dict["url"]))
        resourcedict_list.append(resource_dict)
        return resourcedict_list
    else:
        if debug:
            print("[add_resource_to_list] New resource skipped with duplicate URL {0}".format(
            resource_dict["url"]))
        return resourcedict_list
    
    
def add_resources_to_list(old, new, debug=False):
    """Add multiple resource dicts to a resourcedict_list if URLs are unique
    
    Arguments:
        old (List of dicts): package_show(id="xxx")["resources"]
        new (List of dicts): package_show(id="xxx")["resources"]
        debug (Boolean): Debug noise level
        
    Returns
        List of resource dicts
    """
    result = old
    for resource in new:
        result = add_resource_to_list(result, resource, debug=debug)
    return result

    
def upsert_dataset(data_dict, ckanapi, overwrite_metadata=True, 
                   drop_existing_resources=True, debug=False):
    '''
    Create or update a CKAN dataset (data.wa.gov.au schema) from a dict.

    WARNING: This will overwrite resources and drop manually added resources in CKAN.
    No guarantees can be given for manual changes applied to harvested datasets
    when the dataset is harvested again.

    Arguments:
        datadict (dict): A dict like ckanapi `package_show`
        ckanapi (ckanapi): A ckanapi object (created with CKAN url and write-permitted api key)
        overwrite_metadata (Boolean): Whether to overwrite existing dataset metadata (default)
        drop_existing_resources (Boolean): Whether to drop existing resources (default) or merge
        new and existing with identical resource URL
        debug (Boolean): Debug noise level
    @return None
    '''
    if data_dict is None:
        print("[upsert_dataset] No input, skipping.")
        return(None)
    
    if not data_dict.has_key("name"):
        print("[upsert_dataset] Invalid input:\n{0}".format(str(data_dict)))
        return(None)
    
    n = data_dict.get("name", None)
    
    print("[upsert_dataset] Reading WMS layer {0}".format(n))

    new_package = data_dict
    new_resources = data_dict["resources"]
    
    
    try:
        # Package exists with metadata we want to keep or overwrite,
        # and resources we want to keep or discard
        package = ckanapi.action.package_show(id=n)
        if debug:
            print("[upsert_dataset] Found existing package {0}".format(package["name"]))
        do_update = True
    except:
        print("[upsert_dataset]   Layer not found, creating...")
        do_update = False
        #try:
        package = ckanapi.action.package_create(**data_dict)
        #    print("[upsert_dataset]   Created dataset {0}".format(n))
        #except:
        #    print("[upsert_dataset]   Rejected")
        #    data_dict["ERROR"] = "CKAN rejected layer {0}".format(n)
        #    package = data_dict
        
    
    if do_update:
        
        old_resources = package["resources"]
        if debug:
            print("[upsert_dataset] Old resources: {0}".format(str(old_resources)))
            print("[upsert_dataset] New resources: {0}".format(str(new_resources)))
            

        # Discard or merge existing resources
        if drop_existing_resources:
            resources = new_resources
            msg_res = "[upsert_dataset]  Existing resources were replaced with new resources."
        else:
            resources = add_resources_to_list(old_resources, new_resources, debug=debug)
            msg_res = "[upsert_dataset]  Existing resources were kept, new resources were added."
        
        if debug:
            print("[upsert_dataset]  Merged resources: {0}".format(str(resources)))
        
        # Keep or overwrite package metadata
        if overwrite_metadata:
            pkg = new_package
            msg_pkg = "[upsert_dataset]  Existing dataset metadata were updated."
        else:
            pkg = package
            msg_pkg = "[upsert_dataset]  Existing dataset metadata were not changed."

        # Attach merged or new resources
        pkg["resources"] = resources

        # Update package
        if debug:
            print("[upsert_dataset] Attempting to update package {0} with data\n{1}".format(
                    package["name"], str(package)))
        package = ckanapi.action.package_update(**pkg)
        msg = "[upsert_dataset]  Layer exists.\n  {0}\n  {1}".format(msg_pkg, msg_res)
        print(msg)


    return(package)


def get_pdf_dict(filename):
    """
    Return a spreadsheet of information on PDF resources as a dict.
    The dict contains a list of dataset name:PDF URL key-value pairs.
    """
    print("[get_pdf_dict] Reading {0}...".format(filename))
    with open("data-dictionaries.csv", "rb") as pdflist:
        pdf_dict = dict((p["id"].lower(), 
                         p["url"]) for p in csv.DictReader(pdflist))
    print("[get_pdf_dict] Done.")
    return pdf_dict


def get_org_dict(filename):
    """Return a spreadsheet of organisations as a name-indexed dict of organisation data.
    
    The innermost dicts can be used to `upsert_org` a CKAN organisation.
    The list of dicts can be used to `upsert_orgs`.
    
    The spreadsheet must contain the headers 
    "name","title","url", and "logo_url",
    corresponding to the CKAN organisation keys, 
    plus extras "contact", "url" and "jurisdiction".
    
    Arguments:
        filename The filename incl relative or absolute path of the spreadsheet
        
    Returns:
        A list of dicts to feed `upsert_orgs`.
    """
    print("[get_org_dict] Reading {0}...".format(filename))
    with open(filename, "rb") as orgcsv:
        orgs = dict()
        for org in csv.DictReader(orgcsv):
            orgname = org["name"].lower()
            orgs[orgname] = dict()
            orgs[orgname]["name"] = orgname
            orgs[orgname]["title"] = org["title"]
            orgs[orgname]["url"] = org["url"]
            orgs[orgname]["image_url"] = org["logo_url"]
            orgs[orgname]["groups"] = [{"capacity": "public","name": "wa-state-government"}]
            orgs[orgname]["extras"] = [
                {"key": "Contact", "value": org["contact"]},
                {"key": "Homepage", "value": org["url"]},
                {"key": "Jurisdiction", "value": org["jurisdiction"]}
            ]
                
    print("[get_org_dict] Done.")
    return orgs


def get_group_dict(wms):
    """
    Return a spreadsheet of organisations as a name-indexed dict of organisation data.
    The innermost dicts can be used to `upsert_org` a CKAN organisation.
    
    The spreadsheet must contain the headers "name","title","url", and "logo_url",
    corresponding to the CKAN organisation keys.
    
    Arguments:
        wms (owslib.wms.WebMapService): An owslib WMS instance
    """
    print("[get_group_dict] Reading wms...")
    groups = dict()
    for grp_title in set([wms.contents[l].parent.title for l in wms.contents]):
        groups[grp_title] = dict()
        groups[grp_title]["name"] = slugify(grp_title)
        groups[grp_title]["title"] = grp_title
    print("[get_group_dict] Done.")
    return groups


def upsert_org(datadict, ckanapi, debug=False):
    """Create or update organisations through a ckanapi as per given datadict.
        
    Arguments:
        datadict A dict with information on organizations, such as:
    {
        'logo_url': 'http://www.dfes.wa.gov.au/_layouts/images/FESA.Mobile/dfes_print.png',
        'name': 'dfes',
        'title': 'Department of Fire & Emergency Services',
        'url': 'http://www.dfes.wa.gov.au/'

    }
        ckanapi A ckanapi instance with create_org permissions
    
    Returns:
        A ckanapi organization_show dict
    """
    print("[upsert_org] Upserting organisation {0}, id {1}".format(
            datadict["title"], datadict["name"]))
    if debug:
        print("[upsert_org]   Input:\n{0}".format(str(datadict)))

    try:
        org = ckanapi.action.organization_show(id=datadict["name"])
        print("[upsert_org]   Organisation exists, updating...")
        org = ckanapi.action.organization_update(id=datadict["name"], **datadict)
        print("[upsert_org]   Updated {0}".format(datadict["title"]))

    except:
        print("[upsert_org]   Organisation not found, inserting...")
        org = ckanapi.action.organization_create(**datadict)
        print("[upsert_org]   Inserted {0}".format(datadict["title"]))
    if org:
        return org
    

def upsert_group(datadict, ckanapi, debug=False):
    """Create or update groups through a ckanapi as per given datadict.
        
    Arguments:
        
        datadict A dict with information on groups, such as:
    {
        'name': 'cultural_society_and_demography',
        'title': 'Cultural, Society and Demography'

    }
        ckanapi A ckanapi instance with at least create_group permissions
        
    Returns:
        A ckanapi group_show dict
    """
    print("[upsert_group] Upserting organisation {0}, id {1}".format(
            datadict["title"], datadict["name"]))
    if debug:
        print("[upsert_group]   Input:\n{0}".format(str(datadict)))

    try:
        org = ckanapi.action.group_show(id=datadict["name"])
        print("[upsert_group]   Group exists, updating...")
        org = ckanapi.action.group_update(id=datadict["name"], **datadict)
        print("[upsert_group]   Updated {0}".format(datadict["title"]))

    except:
        print("[upsert_group]   Group not found, inserting...")
        org = ckanapi.action.group_create(**datadict)
        print("[upsert_group]   Inserted {0}".format(datadict["title"]))
    if org:
        return org
    
        
def upsert_orgs(org_dict, ckanapi, debug=False):
    """
    Insert or update CKAN organisations through a ckanapi from an org_dict.
    
    Uses `upsert_org` on each element of `org_dict`.
    Returns a name-indexed dictionary of now existing CKAN organisations.
    This can be used to set the organisation in a CKAN dataset, replacing an expensive 
    `organization_show` with a dictionary lookup.
    """
    print("[upsert_orgs] Refreshing orgs...")
    orgs = [upsert_org(org_dict[org], ckanapi, debug) for org in org_dict]
    print("[upsert_orgs] Done!")
    return dict([o["name"], o] for o in orgs)


def upsert_groups(group_dict, ckanapi, debug=False):
    """Insert or update CKAN groups through a ckanapi from an org_dict.
    
    Uses `upsert_group` on each element of `org_dict`.
    Returns a title-indexed dictionary of now existing CKAN groups.
    This can be used to set the group in a CKAN dataset, replacing an expensive 
    `group_show` with a dictionary lookup.
    """
    print("[upsert_groups] Refreshing groups...")
    groups = [upsert_group(group_dict[grp], ckanapi, debug) for grp in group_dict]
    print("[upsert_groups] Done!")
    return dict([g["title"], g] for g in groups)


def get_layer_dict(wxs, wxs_url, ckanapi, 
                   org_dict, group_dict, pdf_dict, res_format="WMS", 
                   debug=False, fallback_org_name='lgate'):
    """Return a list of CKAN API package_show-compatible dicts
    
    Arguments:
    
        wxs A wxsclient loaded from a WXS enpoint
        wxs_url The WXS endpoint URL to use as dataset resource URL
        ckanapi A ckanapi instance with at least read permission
        org_dict A dict of CKAN org names and ids
        pdf_dict A dict of dataset names and corresponding PDF URLs
        debug Debug noise
        fallback_org_name The fallback CKAN org name , default:'lgate'    
    
    Returns:
        A list of CKAN API package_show-compatible dicts
    """
    foid = ckanapi.action.organization_show(id=fallback_org_name)["id"]
    return [wxs_to_dict(wxs.contents[layername], wxs_url, 
        org_dict, group_dict, pdf_dict, debug=debug,
        res_format=res_format, fallback_org_id=foid) for layername in wxs.contents]


def upsert_datasets(data_dict, ckanapi,overwrite_metadata=True, 
                drop_existing_resources=True, debug=False):
    """Upsert datasets into a ckanapi from data in a dictionary.
    
    Arguments:
        data_dict (dict) An output of `get_layer_dict`
        ckanapi (ckanapi) A ckanapi object (created with CKAN url and write-permitted api key)
        overwrite_metadata (Boolean) Whether to overwrite existing dataset metadata (default)
        drop_existing_resources (Boolean) Whether to drop existing resources (default) or merge
        new and existing with identical resource URL
        debug (Boolean) Debug noise level
        
    Returns:
        A list of `package_show` dicts
    """
    print("Refreshing harvested WMS layer datasets...")
    packages = [upsert_dataset(dataset, 
                               ckanapi,
                               overwrite_metadata=overwrite_metadata, 
                               drop_existing_resources=drop_existing_resources,
                               debug=debug) 
                for dataset 
                in data_dict
                if dataset is not None]
    print("Done!")
    return(packages)


#-------------------------------------------------------------------------------------#
# ArcGIS REST
#-------------------------------------------------------------------------------------#

def get_arc_services(url, foldername):
    """Return a list of service names from an ArcGIS REST folder
    
    Example:
    baseurl = ARCGIS["SLIPFUTURE"]["url"]
    folders = ARCGIS["SLIPFUTURE"]["folders"]
    get_arc_service(baseurl, folders[0])
    ['QC/MRWA_Public_Services']

    res = {
     "currentVersion": 10.31,
     "folders": [],
     "services": [
      {
       "name": "QC/MRWA_Public_Services",
       "type": "MapServer"
      }
     ]
    }


    Arguments:
        url (String): The ArcGIS REST base URL, 
            e.g. 'http://services.slip.wa.gov.au/arcgis/rest/services/'
        foldername (String): The ArcGIS REST service folder name, e.g. 'QC'
        
    Returns:
        A list of strings of service URLs
    """
    res = json.loads(requests.get(os.path.join(url, foldername) + "?f=pjson").content)
    return [os.path.join(url, x) for x in [
            os.path.join(s["name"], s["type"]) for s in res["services"]]]


def get_arc_servicedict(url):
    """Returns a dict of service information for an ArcGIS REST service URL
    
    Arguments
        url (String): An ArcGIS REST service URL, 
            e.g. 'http://services.slip.wa.gov.au/arcgis/rest/services/QC/MRWA_Public_Services/MapServer'
    """
    res = json.loads(requests.get(url + "?f=pjson").content)
    d = dict()
    d["layer_ids"] = [str(x['id']) for x in res["layers"]]
    d["supportedExtensions"] = res["supportedExtensions"]
    return d


def force_key(d, k):
    """Return a key from a dict if existing and not None, else an empty string
    """
    return d[k] if d.has_key(k) and d[k] is not None else ""
    

    
def arcservice_extent_to_gjMP(extent):
    """Transform the extent of an ArcGIS REST service layer into WGS84 and 
    return as GeoJSON Multipolygon Geometry.
    
    Example:
        res = json.loads(requests.get("http://services.slip.wa.gov.au/arcgis/rest/services/"+\
            "QC/MRWA_Public_Services/MapServer/0?f=pjson").content)
        
        print(res["extent"])
        {u'spatialReference': {u'latestWkid': 3857, u'wkid': 102100},
         u'xmax': 14360400.777488748,
         u'xmin': 12639641.896807905,
         u'ymax': -1741902.4945217525,
         u'ymin': -4168751.2292041867}
         
        print arcservice_extent_to_gjMP(res["extent"])
        {"type": "MultiPolygon", 
         "coordinates": [[[
         [113.54383501699999, -15.456807971000012], 
         [113.54383501699999, -35.035829004], 
         [113.54383501699999, -35.035829004], 
         [113.54383501699999, -15.456807971000012]
         ]]]}
         
    
    Arguments:
        extent (dict) The "extent" key of the service layer JSON dict
        
    Returns:
        dict A GeoJSON MultiPolygon Geometry string in WGS84
    """
    inProj = Proj(init='epsg:{0}'.format(str(extent["spatialReference"]["latestWkid"])))
    outProj = Proj(init='epsg:4326')

    xmin = extent["xmin"]
    xmax = extent["xmax"]
    ymin = extent["ymin"]
    ymax = extent["ymax"]

    NW = transform(inProj, outProj, xmin, ymax)
    NE = transform(inProj, outProj, xmax, ymax)
    SW = transform(inProj, outProj, xmin, ymin)
    SE = transform(inProj, outProj, xmax, ymin)

    w, n, e, s = NW[0], NW[1], SE[0], SE[1]
    
    return json.dumps({"type": "MultiPolygon", "coordinates": [[[[e,n],[e,s],[w,s],[w,n]]]]})
    
    
def parse_argis_rest_layer(layer_id, services, base_url, ckan, 
                           owner_org_id=None, author=None, author_email=None, 
                           fallback_org_name='lgate', debug=False):
    """Parse an ArcGIS REST layer into a CKAN package dict of data.wa.gov.au schema
    
    Arguments:
        layer_id (String): The ArcGIS REST layer id
        services (String): A comma separated list of ArcGIS REST services available 
            for the given layer as per its parent service definition,
            e.g. 'WFSServer, WMSServer'
        base_url (String): The ArcGIS REST service URL, 
            e.g. 'http://services.slip.wa.gov.au/arcgis/rest/services/QC/MRWA_Public_Services/MapServer/'
        ckan (ckanapi.RemoteCKAN) An instance of ckanapi.RemoteCKAN
        owner_org_id (String) The CKAN owner org ID, optional, default: fallback to lgate's ID
        author (String): The dataset author, optional
        author_email (String): The dataset author email, optional
        fallback_org_name (String) The CKAN owner org name, default: 'lgate'
        debug (Boolean): Debug noise level
    
    Returns:
        A dictionary in format ckanpai.action.package_show(id=xxx)
    """
    layer_url = os.path.join(base_url, layer_id)
    res = json.loads(requests.get(layer_url + "?f=pjson").content)
    
    # Assumptions!
    desc_preamble = """This dataset has been harvested from [Locate WA](http://locate.wa.gov.au/).\n\n"""
    date_pub = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    
    if not owner_org_id:
        owner_org_id = ckan.action.organization_show(id=fallback_org_name)["id"]
    
    
    # Splitting description into a description dict dd
    
    dd = dict([z.strip().replace(":","-") for z in x.split(":",1)] for x in res["description"].split("\n\n"))
    """
    {u'Abstract': u'All guide signs under the responsibility of Main Roads Western Australia. A guide sign is a type of traffic sign that used to indicate locations, distances, directions, routes, and similar information. One type of guide sign is route marker or an exit sign on a freeway.',
     u'Geographic Extent': u'WA',
     u'Legal Constraints': u"The licensee of this data only acquires the right to use this data for Main Roads' business only, and does not acquire any rights of ownership of the data. This data must not be supplied for third party use without the express written permission of the licensor. The data is made available in good faith and is derived from sources believed to be reliable and accurate. Nevertheless, the reliability and accuracy of the data supplied cannot be guaranteed and Main Roads, its employees and agents expressly disclaim liability for any act or omission done in reliance on the data provided or for any consequences, whether direct or indirect, of any such act or omission. The licensee also shall not release or provide any information that might specifically identify a person or persons from the data provided.",
     u'Main Roads Contact Email': u'irissupport@mainroads.wa.gov.au',
     u'Main Roads Contact Name': u'IRIS Support',
     u'Original Source': u'Main Roads Western Australia',
     u'Other Constraints': u'There are no other constraints for this dataset',
     u'Purpose': u'This layer shows the location of guide signs where Main Roads Western Australia is responsible. Signs can be on the State Road Network or other public access roads and is provided for information only.',
     u'Road Inventory': u'Signs - Guide',
     u'Tags': u'transport, Main Roads Western Australia,mrwa, road,download,public,transportation,Classification,State Road,Main Road,network, wfs:mrwa',
     u'Usage Constraints': u'There are no usage constraints for this dataset',
     u'Usage Limitation': u"The Licensee acknowledges that no warranties or undertakings express or implied, statutory or otherwise, as to the condition, quality or fitness for the Licensee's purposes are provided with this information. It is the responsibility of the Licensee to ensure that the information supplied meets their own requirements. All attributes contained within datasets is provided as is.",
     u'Visible Scale Range': u'Layer displays at all scales.'}
    """
    abstract = force_key(dd, "Abstract")
    extent = force_key(dd, "Geographic Extent")
    legal =  force_key(dd, "Legal Constraints")
    source = force_key(dd, "Original Source")
    tags = force_key(dd, "Tags")
    # and so on
    
    tag_string = [x.strip() for x in tags.split(",")] + ["SLIP Future", "Harvested"]
    
    d = dict()
    
    d["name"] = slugify(res["name"])
    d["title"] = res["name"].replace("_"," ")
    #d["doi"] = ""
    #d["citation"] = ""
    d["notes"] = desc_preamble + res["description"]
    d["tag_string"] = tag_string
    d["owner_org"] =  owner_org_id
    d["data_portal"] = "http://locate.wa.gov.au/"
    d["data_homepage"] = layer_url
    d["license_id"] = "cc-by-sa"
    d["author"] = author if author else source if source else "Landgate"
    d["author_email"] = author_email if author_email else "customerservice@landgate.wa.gov.au"
    d["maintainer_email"] = "customerservice@landgate.wa.gov.au"
    d["maintainer"] = "Landgate"
    d["private"] = False
    d["state"] = "active"
    d["spatial"] = arcservice_extent_to_gjMP(res["extent"])
    """
    #hardcode WA extent:
    d["spatial"] =  json.dumps({"type": "MultiPolygon", 
                    "coordinates": [
                                [[[128.84765625000003, -11.523087506868514], 
                                  [128.67187500000003, -34.88593094075316], 
                                  [114.43359375000001, -37.020098201368114], 
                                  [110.91796875000001, -19.973348786110602], 
                                  [128.84765625000003, -11.523087506868514]]]]})
    """
    d["published_on"] = date_pub
    d["last_updated_on"] = date_pub
    d["update_frequency"] = "frequent"
    #d["data_temporal_extent_begin"] = ""
    #d["data_temporal_extent_end"] = ""

    resource_list = []
    
    
    # Attach WMS/WFS endpoint as resource
    if "WMSServer" in services:
        r = dict()
        r["description"] = "OGC Web Map Service Endpoint"
        r["format"] = "wms"
        r["name"] = "{0} WMS".format(res["name"])
        r["url"] = os.path.join(base_url, "WMSServer")
        r["wms_layer"] = layer_id
        resource_list.append(r)
    
    if "WFSServer" in services:
        r = dict()
        r["description"] = "OGC Web Feature Service Endpoint"
        r["format"] = "wfs"
        r["name"] = "{0} WFS".format(res["name"])
        r["url"] = os.path.join(base_url, "WFSServer")
        r["wfs_layer"] = layer_id
        resource_list.append(r)
    
    d["resources"] = resource_list
    
    if debug:
        print("[parse_argis_rest_layer] Returning package dict \n{0}".format(str(d)))

    return d

def harvest_arcgis_service(service_url, ckan, owner_org_id, author, author_email, 
                           overwrite_metadata=True, drop_existing_resources=True,debug=False):
    """Harvest all layers underneath an ArcGIS REST Service URL into a CKAN
    
    Arguments:
        service_url (String): The ArcGIS REST service URL, 
            e.g. 'http://services.slip.wa.gov.au/arcgis/rest/services/QC/MRWA_Public_Services/MapServer/'
        ckan (ckanapi.RemoteCKAN) An instance of ckanapi.RemoteCKAN
        owner_org_id (String) The CKAN owner org ID, optional
        author (String): The dataset author, optional
        author_email (String): The dataset author email, optional
        fallback_org_name (String) The CKAN owner org name, default: 'lgate'
        overwrite_metadata (Boolean) Whether to overwrite existing dataset metadata (default)
        drop_existing_resources (Boolean) Whether to drop existing resources (default) or merge
        debug (Boolean): Debug noise level
    """
    servicedict = get_arc_servicedict(service_url)
    for layer in servicedict["layer_ids"]:
        print("\n\nParsing layer {0}".format(layer))
        ds_dict = parse_argis_rest_layer(layer, 
                                         servicedict["supportedExtensions"], 
                                         service_url, 
                                         ckan,
                                         owner_org_id = owner_org_id,
                                         author = author,
                                         author_email = author_email,
                                         debug=debug)
        print("Writing dataset {0}...".format(ds_dict["title"]))
        if debug:
            print(ds_dict)
        ckan_ds = upsert_dataset(ds_dict, 
                                 ckan, 
                                 overwrite_metadata = overwrite_metadata,
                                 drop_existing_resources = drop_existing_resources, 
                                 debug=debug)
        if debug:
            print(ckan_ds)
        print("Upserted dataset {0} to CKAN {1}".format(ckan_ds["title"], ckan.address))
