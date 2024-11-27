const dropArea = document.getElementById('dropArea');
const uploadButton = document.querySelector('label[for="fileToUpload"]');
const fileUploadButton = document.getElementById('fileToUpload');
const fileNameSpan = document.getElementById('fileName');

['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, preventDefaults, false);
});

function preventDefaults(e) {
    e.preventDefault();
    e.stopPropagation();
}

['dragenter', 'dragover'].forEach(eventName => {
    dropArea.addEventListener(eventName, highlight, false);
});

['dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, unhighlight, false);
});

function highlight(e) {
    dropArea.classList.add('highlight');
}

function unhighlight(e) {
    dropArea.classList.remove('highlight');
}

dropArea.addEventListener('drop', handleDrop, false);

function handleDrop(e) {
    const dt = e.dataTransfer;
    const files = dt.files;
    
    document.getElementById('fileToUpload').files = files;
    // console.log("------------")
    // console.log(files)
    // console.log("------------")

    handleFiles(files);
}

fileUploadButton.onchange = function() {
    const files = document.getElementById('fileToUpload').files;
    handleFiles(files);
}

// function handleFiles(files) {
//     console.log(files.length)
//     for (let i = 0; i < files.length; i++) {
//         const file = files[i];
//         console.log('File:', file);
//         // You can perform further actions with the dropped files here
//     }
// }

function handleFiles(files) {
    if (files.length > 0) {
        const file = files[0];
        console.log('File:', file);
        fileNameSpan.textContent = file.name;
        fileInfo.style.display = 'block';
        dropArea.style.display = 'none';
    }
}
// function uploadFiles(e) {
//     const files = document.getElementById('fileToUpload').files;
//     console.log(files);
//     fileInfo.style.display = 'none';
//     dropArea.style.display = 'block';

//     var file = files[0];
//     console.log(file);

//     var apigClient = apigClientFactory.newClient();

//     var params = {
//         "key": `${Date.now()}_${file.name.replace(/\s+/g, '_')}`,
//         "bucket": "assignment3-b2-photos",
//         "x-amz-meta-customLabels": document.getElementById("tags").value,
//         "Content-Type": file.type
//     };
    
//     var body = file; // Use raw file content directly
//     console.log("Custom Labels:", params['x-amz-meta-customLabels']);

//     apigClient
//         .uploadBucketKeyPut(params, body, {
//             'Access-Control-Allow-Origin': '*',
//             'x-amz-meta-customLabels': params['x-amz-meta-customLabels'],
//             'Content-Type': file.type,
//         })
//         .then(function (res) {
//             if (res.status === 200) {
//                 console.log("Uploaded successfully");
//                 console.log(res);
//                 toggleText("success");
//             }
//         })
//         .catch((err) => {
//             console.log("Upload failed");
//             console.log(err);
//             toggleText("failure");
//         });
// }

function uploadFiles(e) {
    const files = document.getElementById('fileToUpload').files;
    console.log(files);
    fileInfo.style.display = 'none';
    dropArea.style.display = 'block';
    console.log(files);
    var file = files[0];
    getBase64(file).then((data) => {
        console.log(data);
        var apigClient = apigClientFactory.newClient();

        var fileType = file.type + ';base64';
        console.log(fileType)
        var body = data;
        
        var customLabels = document.getElementById("tags").value;
        var params = {
            "key": Date.now().toString() + file.name,
            'x-amz-meta-customLabels': customLabels,
                'Content-Type': file.type,
                "bucket": ""
        };
        console.log("Custom Labels:", customLabels);
        apigClient
            .uploadBucketKeyPut(params, body, {
                'Access-Control-Allow-Origin': '*',
                'x-amz-meta-customLabels': customLabels,
                'Content-Type': file.type,
            })
            .then(function (res) {
                if (res.status == 200) {
                    console.log("Uploaded successfully")
                    console.log(res)
                    toggleText("success")
                }
            }).catch((err) => {
                console.log("Upload failed")
                console.log(err)
                toggleText("failure")
            }
        );
    });
}

function getBase64(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.readAsDataURL(file);
        reader.onload = () => {
            let encoded = reader.result.replace(/^data:(.*;base64,)?/, '');
            if (encoded.length % 4 > 0) {
                encoded += '='.repeat(4 - (encoded.length % 4));
            }
            resolve(encoded);
        };
        reader.onerror = (error) => reject(error);
    });
}


function toggleText(text) {
    var text = document.getElementById(text);
    text.style.display = "block"; 
    setTimeout(function(){
      text.style.display = "none"; 
    }, 5000); 
}

function searchFiles() {
    const searchValue = document.getElementById('search').value;
    document.getElementById('img-container').innerHTML = "";
    console.log(searchValue);
    var apigClient = apigClientFactory.newClient();
    var params = {
        "q": searchValue
    };
    apigClient
        .searchGet(params, {}, {
            'Access-Control-Allow-Origin': '*'
        })
        .then(function (res) {
            if (res.status == 200) {
                console.log("Search successfully")
                console.log(res)
                var images = res["data"]["images"]
                images.forEach(image=>{
                    // var url = 'https://s3.amazonaws.com/' + image;
                    var url = image;
                    fetch(url, { mode: 'cors', method: "GET"}).then(resp=>{
                        console.log(url);
                        console.log(resp);
                        if(resp.ok) {
                            return resp.text();
                        }
                        console.log("---------------------")
                    }, err=> {
                        console.log("Error", url, err)
                    }). then(base64Data=>{
                        // base64Data = JSON.parse(base64Data)["body-json"]
                        console.log(base64Data)
                        
                        const img = new Image();
                        img.src = `data:image/jpeg;base64,${base64Data}`;
                        img.setAttribute('class', 'banner-img');
                
                        document.getElementById('img-container').appendChild(img);
                    }) 
                })
            }
        }).catch((err) => {
            console.log("Search failed")
            console.log(err)
        }
    );
}