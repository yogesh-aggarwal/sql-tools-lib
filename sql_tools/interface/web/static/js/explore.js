const request = () => {
  const xhr = new XMLHttpRequest();
  xhr.open("GET", "http://localhost:5000/api", true);
  xhr.onload = async () => {
    console.log("ok");
    if (xhr.status === 200) {
      manipulate(JSON.parse(xhr.responseText));
    }
  };
  xhr.onreadystatechange = async () => {
    setTimeout(() => {
      xhr.open("GET", "http://localhost:5000/api", true);
      xhr.send();
    }, 100);
  };
  xhr.send();
};

const manipulate = (data) => {
  document.querySelector("#main").innerText = data["time"];
};

// Main thread
request();
