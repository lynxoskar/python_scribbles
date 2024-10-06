import argparse
import time
import requests



def make_http_call(uri, proxy, throttle_time, num_calls):
    proxies = {
        "http": proxy,
        "https": proxy,
    }
    
    for _ in range(num_calls):
        try:
            print(f"Making request to {uri} with proxy {proxy}")
          
            # Prepare the request
            req = requests.Request('GET', uri)
            prepared = req.prepare()
            
            # Send the prepared request
            session = requests.Session()
            response = session.send(prepared, proxies=proxies)
            
            # Output the actual request text
            request_text = f"{prepared.method} {prepared.url} HTTP/1.1\r\n" + \
                           "\r\n".join(f"{k}: {v}" for k, v in prepared.headers.items()) + \
                           "\r\n\r\n" + (prepared.body or "")
            print(f"Request sent to proxy:\n{request_text}")
            
            # Print the response
            print(f"Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            print(f"Status Code: {response.status_code}")
            print(f"Response: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        
        time.sleep(throttle_time)

def main():
    parser = argparse.ArgumentParser(description="Make an HTTP call using a proxy.")
    parser.add_argument("--uri", type=str, help="The URI to call.", required=False, default="http://localhost:8000/tinystatus")
    parser.add_argument("--proxy", type=str, help="The proxy to use for the HTTP call.", required=False, default="http://localhost:8080")
    parser.add_argument("--num_calls", type=int, help="The number of times to call the URI.", required=False, default=1)
    parser.add_argument("--throttle_time", type=int, help="The time to throttle between calls (in seconds).", required=False, default=1)
    
    args = parser.parse_args()
    
    make_http_call(args.uri, args.proxy, args.throttle_time, args.num_calls)

if __name__ == "__main__":   
    main()