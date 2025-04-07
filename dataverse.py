import time
import requests
import json
from dotenv import load_dotenv
import os
import urllib.parse
import msal
import base64

# Load credentials from .env file
load_dotenv()

# Retrieve credentials from .env file
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
RESOURCE_URL = os.getenv("RESOURCE_URL")
AUTHORITY_URL = os.getenv("AUTHORITY_URL")


# Get access token for Dataverse
def get_access_token():
    """Authenticate with Azure AD and get an access token for Dataverse."""
    dataverse_scope = [f"{RESOURCE_URL}/.default"]
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY_URL,
        client_credential=CLIENT_SECRET
    )
    try:
        # Acquire token for dataverse access
        result = app.acquire_token_for_client(scopes=dataverse_scope)

        if "access_token" in result:
            print("Dataverse access token acquired successfully!")
            return result["access_token"]
        else:
            error_description = result.get("error_description", "No error description provided.")
            print(f"Failed to acquire Dataverse token: {error_description}")
            exit(1)
    except Exception as e:
        print(f"Dataverse: Error during authentication: {e}")

def download_and_convert_to_base64(image_url):
    """Download an image from the given URL and convert it to a Base64 string."""
    try:
        print(f"Attempting to download image from: {image_url}")  # Log the URL
        response = requests.get(image_url)
        response.raise_for_status()
        # Convert image to Base64
        base64_image = base64.b64encode(response.content).decode("utf-8")
        print(f"Successfully downloaded and converted image: {image_url}")
        return base64_image
    except requests.exceptions.RequestException as e:
        print(f"Error downloading image from {image_url}: {e}")
        return None


def fetch_business_units_and_related_data():
    """Fetch business units and related data from the crd8d_qr2 table, including blob images."""
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": 'odata.include-annotations="OData.Community.Display.V1.FormattedValue"',
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }

    # Base URL for Azure Blob Storage
    base_url = "https://tnbsl.blob.core.windows.net/qr2image/"

    # Month mapping
    month_mapping = {
        "01": "JAN", "JANUARY": "JAN", "JAN": "JAN",
        "02": "FEB", "FEBRUARY": "FEB", "FEB": "FEB",
        "03": "MAR", "MARCH": "MAR", "MAR": "MAR",
        "04": "APR", "APRIL": "APR", "APR": "APR",
        "05": "MAY", "MAY": "MAY",
        "06": "JUN", "JUNE": "JUN", "JUN": "JUN",
        "07": "JUL", "JULY": "JUL", "JUL": "JUL",
        "08": "AUG", "AUGUST": "AUG", "AUG": "AUG",
        "09": "SEP", "SEPTEMBER": "SEP", "SEP": "SEP",
        "10": "OCT", "OCTOBER": "OCT", "OCT": "OCT",
        "11": "NOV", "NOVEMBER": "NOV", "NOV": "NOV",
        "12": "DEC", "DECEMBER": "DEC", "DEC": "DEC"
    }

    # Get user input for business unit names, year, and month
    business_unit_names_input = input("Enter the business unit names (comma-separated): ").upper()  # Auto-capitalize
    year_input = input("Enter the year (e.g., 2024): ")
    month_input = input("Enter the month (e.g., March/Mar/03): ").strip().upper()

    if month_input in month_mapping:
        month_numeric = [key for key, value in month_mapping.items() if value == month_mapping[month_input]][0]  # Get numeric format
        month_name = month_mapping[month_input]  # Get three-letter abbreviation
        print(f"Month numeric: {month_numeric}, Month name: {month_name}")
    else:
        print(f"Invalid month input: {month_input}. Please enter a valid month.")
        exit(1)

    # the checkpoint file name
    CHECKPOINT_FILE = f"processed_business_units_{month_name}_{year_input}.json"

    def load_checkpoint():
        """Load the list of processed business units from the checkpoint file."""
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as file:
                return json.load(file)
        return []

    def save_checkpoint(processed_units):
        """Save the list of processed business units to the checkpoint file."""
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as file:
            json.dump(processed_units, file, indent=4)


    # Split the input into a list of business unit names
    business_unit_names = [name.strip() for name in business_unit_names_input.split(",")]

    # FetchXML query to retrieve business unit data
    fetchxml_query_business_units = """
    <fetch top="5000">
      <entity name="businessunit">
        <attribute name="name" />
        <attribute name="businessunitid" />
        <attribute name="crd8d_admin" />
        <attribute name="crd8d_chargeman" />
        <attribute name="crd8d_chargemanno" />
        <attribute name="crd8d_ssmregistrationno" />
        <attribute name="crd8d_tnbvendorno" />
        <link-entity name="crd8d_zon" from="crd8d_zonid" to="crd8d_zon" link-type="inner" alias="zon">
          <attribute name="crd8d_businessarea" />
          <attribute name="crd8d_sub_business_area" />
          <attribute name="crd8d_engineer" />
          <attribute name="crd8d_engineerno" />
          <attribute name="crd8d_kod" />
          <attribute name="crd8d_negeri" />
          <attribute name="crd8d_region" />
          <attribute name="crd8d_technician" />
          <attribute name="crd8d_technicianno" />
          <attribute name="crd8d_weekend" />
        </link-entity>
      </entity>
    </fetch>
    """

    # URL-encode the FetchXML query
    encoded_fetchxml_query_business_units = urllib.parse.quote(fetchxml_query_business_units)

    # Define the Dataverse API endpoint for FetchXML
    fetchxml_endpoint_business_units = f"{DATAVERSE_URL}/api/data/v9.2/businessunits?fetchXml={encoded_fetchxml_query_business_units}"

    try:
        # Fetch business units
        response = requests.get(fetchxml_endpoint_business_units, headers=headers)
        response.raise_for_status()
        business_units = response.json().get("value", [])

        print(f"Total business units retrieved: {len(business_units)}")

        # Load the checkpoint
        processed_units = load_checkpoint()

        # Process each business unit
        matching_business_units = []
        for business_unit in business_units:
            business_unit_id = business_unit.get("businessunitid", "unknown_id")
            business_unit_name = business_unit.get("name", "Unknown")

            # Check if the business unit name contains any of the user input names
            if any(name in business_unit_name for name in business_unit_names):
                matching_business_units.append(business_unit)
                print(f"Found matching business unit: {business_unit_name}")

        if not matching_business_units:
            print(f"No matching business unit found for input: {business_unit_names_input}")
            return

        # Process each matching business unit
        for business_unit in matching_business_units:
            business_unit_id = business_unit.get("businessunitid", "unknown_id")
            business_unit_name = business_unit.get("name", "Unknown")

            # Skip if the business unit has already been processed
            if any(unit["business_unit_id"] == business_unit_id for unit in processed_units):
                print(f"Skipping already processed business unit: {business_unit_name}")
                continue

            print(f"Processing business unit: {business_unit_name}")

            # FetchXML query for qr2 table
            fetchxml_query_crd8d_qr2 = f"""
            <fetch top="5">
             <entity name="crd8d_qr2">
                <attribute name="owningbusinessunit" />
                <attribute name="crd8d_audit" />
                <attribute name="crd8d_carakawalan" />
                <attribute name="crd8d_catatan" />
                <attribute name="crd8d_gantipecu" />
                <attribute name="crd8d_id" />
                <attribute name="crd8d_jenamalampu" />
                <attribute name="crd8d_jenamaxlampu" />
                <attribute name="crd8d_jenamaxpecu" />
                <attribute name="crd8d_jeniskerja" />
                <attribute name="crd8d_jenislampu" />
                <attribute name="crd8d_jenispemasanganbaru" />
                <attribute name="crd8d_jenisxlampu" />
                <attribute name="crd8d_kapasiti" />
                <attribute name="crd8d_kategorilampu" />
                <attribute name="crd8d_kodlampu" />
                <attribute name="crd8d_kodxlampu" />
                <attribute name="crd8d_lampumanyala" />
                <attribute name="crd8d_lampurosak" />
                <attribute name="crd8d_location" />
                <attribute name="crd8d_lokasikawalanmasa" />
                <attribute name="crd8d_namajalan" />
                <attribute name="crd8d_namapemegangakaun" />
                <attribute name="crd8d_noakaun" />
                <attribute name="crd8d_noreport" />
                <attribute name="crd8d_nosiri" />
                <attribute name="crd8d_nosn" />
                <attribute name="crd8d_notiang" />
                <attribute name="crd8d_perihalkerja" />
                <attribute name="crd8d_qr2id" />
                <attribute name="crd8d_tahunxpecu" />
                <attribute name="crd8d_tarikh" />
                <attribute name="crd8d_waktu" />
                <attribute name="createdby" />
                <attribute name="createdon" />
                <attribute name="modifiedby" />
                <attribute name="modifiedon" />
                <attribute name="crd8d_workorderstatus" />
                <attribute name="crd8d_gambar1_blob" />
                <attribute name="crd8d_gambar2_blob" />
                <attribute name="crd8d_gambar3_blob" />
                <attribute name="crd8d_gambar4_blob" />
                <attribute name="crd8d_gambar5_blob" />

                <filter type="and">
                  <condition attribute="owningbusinessunit" operator="eq" value="{business_unit_id}" />
                  <condition attribute="crd8d_tarikh" operator="on-or-after" value="{year_input}-{month_numeric}-01" />
                  <condition attribute="crd8d_tarikh" operator="on-or-before" value="{year_input}-{month_numeric}-31" />
                </filter>
              </entity>
            </fetch>
            """

            # URL-encode the FetchXML query
            encoded_fetchxml_query_crd8d_qr2 = urllib.parse.quote(fetchxml_query_crd8d_qr2)

            # Define the Dataverse API endpoint for FetchXML
            fetchxml_endpoint_crd8d_qr2 = f"{DATAVERSE_URL}/api/data/v9.2/crd8d_qr2s?fetchXml={encoded_fetchxml_query_crd8d_qr2}"

            # Fetch related data from crd8d_qr2 table
            response_qr2 = requests.get(fetchxml_endpoint_crd8d_qr2, headers=headers)
            response_qr2.raise_for_status()
            related_data = response_qr2.json().get("value", [])
            
            if not related_data:
                print(f"No data retrieved for business unit '{business_unit_name}'.")
            else:
                # Log the number of rows retrieved
                print(f"Total rows retrieved for business unit '{business_unit_name}': {len(related_data)}")

            for row in related_data:
                for i in range(1, 6):  # Loop through gambar1_blob to gambar5_blob
                    blob_attribute = f"crd8d_gambar{i}_blob"
                    if blob_attribute in row and row[blob_attribute]:
                        image_url = f"{base_url}{row[blob_attribute]}"
                        base64_image = download_and_convert_to_base64(image_url)
                        if base64_image:
                            # Store the Base64 string with a new key
                            row[f"gambar{i}_base64"] = base64_image
                        else:
                            print(f"Failed to process image: {image_url}")

            # Define the mapping of old keys to new variable names
            key_mapping = {
                "crd8d_audit": "audit",
                "crd8d_catatan": "catatan",
                "crd8d_gantipecu@OData.Community.Display.V1.FormattedValue": "gantipecu",
                "crd8d_id": "id",
                "crd8d_jenamalampu@OData.Community.Display.V1.FormattedValue": "jenamalampu",
                "crd8d_jenamaxlampu@OData.Community.Display.V1.FormattedValue": "jenamaxlampu",
                "crd8d_jenamaxpecu@OData.Community.Display.V1.FormattedValue": "jenamaxpecu",
                "crd8d_jeniskerja@OData.Community.Display.V1.FormattedValue": "jeniskerja",
                "crd8d_jenislampu@OData.Community.Display.V1.FormattedValue": "jenislampu",
                "crd8d_jenispemasanganbaru@OData.Community.Display.V1.FormattedValue": "jenispemasanganbaru",
                "crd8d_jenisxlampu@OData.Community.Display.V1.FormattedValue": "jenisxlampu",
                "crd8d_kapasiti@OData.Community.Display.V1.FormattedValue": "kapasiti",
                "crd8d_kategorilampu@OData.Community.Display.V1.FormattedValue": "kategorilampu",
                "crd8d_kodlampu@OData.Community.Display.V1.FormattedValue": "kodlampu",
                "crd8d_kodxlampu@OData.Community.Display.V1.FormattedValue": "kodxlampu",
                "crd8d_lampumanyala@OData.Community.Display.V1.FormattedValue": "lampumanyala",
                "crd8d_lampurosak@OData.Community.Display.V1.FormattedValue": "lampurosak",
                "crd8d_location": "location",
                "crd8d_namajalan": "namajalan",
                "crd8d_noreport": "noreport",
                "crd8d_nosiri": "nosiri",
                "crd8d_notiang": "notiang",
                "crd8d_perihalkerja": "perihalkerja",
                "crd8d_qr2id": "qr2id",
                "crd8d_tahunxpecu@OData.Community.Display.V1.FormattedValue": "tahunxpecu",
                "crd8d_tarikh": "tarikh",
                "crd8d_waktu@OData.Community.Display.V1.FormattedValue": "waktu",
                "createdon": "createdon",
                "modifiedon": "modifiedon",
                "crd8d_nosn": "nosn",
                "crd8d_carakawalan@OData.Community.Display.V1.FormattedValue": "carakawalan",
                "_createdby_value@OData.Community.Display.V1.FormattedValue": "_createdby_value",
                "_modifiedby_value@OData.Community.Display.V1.FormattedValue": "_modifiedby_value",
                "_owningbusinessunit_value@OData.Community.Display.V1.FormattedValue": "_owningbusinessunit_value",
                "crd8d_lokasikawalanmasa": "lokasikawalanmasa",
                "crd8d_namapemegangakaun": "namapemegangakaun",
                "crd8d_noakaun": "noakaun",
                "_crd8d_admin_value@OData.Community.Display.V1.FormattedValue": "admin_name",
                "_crd8d_chargeman_value@OData.Community.Display.V1.FormattedValue": "chargeman_name",
                "crd8d_chargemanno": "chargeman_number",
                "crd8d_ssmregistrationno": "ssm_registration_number",
                "crd8d_tnbvendorno": "tnb_vendor_number",
                "zon.crd8d_businessarea": "business_area",
                "zon.crd8d_sub_business_area": "sub_business_area",
                "zon.crd8d_negeri@OData.Community.Display.V1.FormattedValue": "negeri",
                "zon.crd8d_kod": "kod",
                "zon.crd8d_region@OData.Community.Display.V1.FormattedValue": "region",
                "zon.crd8d_engineer@OData.Community.Display.V1.FormattedValue": "engineer_name",
                "zon.crd8d_engineerno": "engineer_number",
                "zon.crd8d_technician@OData.Community.Display.V1.FormattedValue": "technician_name",
                "zon.crd8d_technicianno": "technician_number",
                "zon.crd8d_weekend@OData.Community.Display.V1.FormattedValue": "weekend",
                "crd8d_workorderstatus@OData.Community.Display.V1.FormattedValue": "workorderstatus",
                "gambar1_base64": "image1_full",
                "gambar2_base64": "image2_full",
                "gambar3_base64": "image3_full",
                "gambar4_base64": "image4_full",
                "gambar5_base64": "image5_full"
            }

            def remap_and_save_data(related_data, business_unit, key_mapping, month_name):
                """Combine business unit data with related data, remap keys, and save into JSON files."""
                business_unit_name = business_unit.get("name", "Unknown")
                business_unit_id = business_unit.get("businessunitid", "unknown_id")
                
                # Retrieve negeri for each kkb 
                business_unit_negeri = business_unit.get("zon.crd8d_negeri@OData.Community.Display.V1.FormattedValue", "Unknown")
                # Initialize total rows processed
                total_rows_processed = 0

                # Check if there is any data to process
                if not related_data:
                    print(f"No data retrieved for business unit '{business_unit_name}'. No file will be saved.")
                else:
                    combined_data = []
                    file_name = f"{business_unit_name}_{business_unit_negeri}_{month_name}_{year_input}.json"  # Use month name here
                    for index, row in enumerate(related_data):
                        try:
                            # Combine business unit data with related data
                            combined_row = {**business_unit, **row}

                            # Remap keys
                            remapped_row = {new_key: combined_row.get(old_key, "") for old_key, new_key in key_mapping.items()}

                            # Add the remapped row to the combined data list
                            combined_data.append(remapped_row)

                            # Log the row being processed
                            print(f"Appending row {index + 1} for business unit '{business_unit_name}'.")

                            # Save the remapped row into the JSON file
                            if not os.path.exists(file_name):
                                with open(file_name, "w", encoding="utf-8") as json_file:
                                    json.dump([remapped_row], json_file, indent=4)
                            else:
                                with open(file_name, "r+", encoding="utf-8") as json_file:
                                    existing_data = json.load(json_file)
                                    existing_data.append(remapped_row)
                                    json_file.seek(0)
                                    json.dump(existing_data, json_file, indent=4)

                            # Increment the total rows processed
                            total_rows_processed += 1

                        except Exception as e:
                            # Log any issues while processing the row
                            print(f"Error processing row {index + 1} for business unit '{business_unit_name}': {e}")

                    # Log the total number of rows saved in the final JSON file
                    try:
                        with open(file_name, "r", encoding="utf-8") as json_file:
                            final_data = json.load(json_file)
                            print(f"Total rows saved in '{file_name}': {len(final_data)}")
                    except Exception as e:
                        print(f"Error reading final JSON file '{file_name}': {e}")

                # Update the checkpoint
                processed_units = load_checkpoint()  # Load the current checkpoint
                if not any(unit["business_unit_id"] == business_unit_id for unit in processed_units):
                    # add kkb ID, name, and total rows processed
                    processed_units.append({
                        "business_unit_id": business_unit_id,
                        "business_unit_name": business_unit_name,
                        "total_rows": total_rows_processed
                    })
                else:
                    # Update the existing entry with the total rows processed
                    for unit in processed_units:
                        if unit["business_unit_id"] == business_unit_id:
                            unit["total_rows"] = total_rows_processed
                            break

                save_checkpoint(processed_units)  # Save the updated checkpoint

                if total_rows_processed > 0:
                    print(f"Saved combined data for business unit '{business_unit_name}' and updated checkpoint.")
                else:
                    print(f"No data processed for business unit '{business_unit_name}', but checkpoint updated.")

            # Call the remap_and_save_data function
            remap_and_save_data(related_data, business_unit, key_mapping, month_name)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    fetch_business_units_and_related_data()