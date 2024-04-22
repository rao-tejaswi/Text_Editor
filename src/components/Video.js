// Video.js
import Avatar from 'react-avatar';
import React, { useEffect, useRef, useState } from 'react';

const Video = ({ socket, roomId, username,isLocalUser }) => {
    const localVideoRef = useRef();

    const remoteVideoRef = useRef();
    const peerConnection = new RTCPeerConnection();
    const [videoEnabled, setVideoEnabled] = useState(true);

    useEffect(() => {
        // Get user media and display it locally
        navigator.mediaDevices.getUserMedia({ video: true, audio: true })
            .then((stream) => {
                localVideoRef.current.srcObject = stream;
                peerConnection.addStream(stream);
            })
            .catch((error) => console.error('Error accessing media devices:', error));

        // Listen for remote stream and display it
        peerConnection.onaddstream = (event) => {
            remoteVideoRef.current.srcObject = event.stream;
        };

        // Listen for ICE candidates and send them to the signaling server
        peerConnection.onicecandidate = (event) => {
            if (event.candidate) {
                socket.emit('ice-candidate', { roomId, candidate: event.candidate });
            }
        };

        // Listen for signaling events (offer, answer)
        socket.on('offer', (data) => {
            peerConnection.setRemoteDescription(new RTCSessionDescription(data.offer));
            peerConnection.createAnswer()
                .then((answer) => {
                    peerConnection.setLocalDescription(answer);
                    socket.emit('answer', { roomId, answer });
                })
                .catch((error) => console.error('Error creating answer:', error));
        });

        socket.on('answer', (data) => {
            peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
        });

        // Create offer and emit it to the signaling server
        peerConnection.createOffer()
            .then((offer) => {
                peerConnection.setLocalDescription(offer);
                socket.emit('offer', { roomId, offer });
            })
            .catch((error) => console.error('Error creating offer:', error));

    }, []);

    const toggleVideo = () => {
        setVideoEnabled((prev) => !prev);
        const videoTrack = localVideoRef.current.srcObject.getVideoTracks()[0];
        videoTrack.enabled = !videoTrack.enabled;
    };

    return (
        <div className='videoContainer' >
            <span className="userName">{username}</span>
            <div >
                {/* <video className='videoFeature' ref={localVideoRef} autoPlay playsInline muted /> */}
                {videoEnabled ? (
                <video className='videoFeature' ref={localVideoRef} autoPlay playsInline muted />
                ) : (
                    <Avatar className='videoFeature' name={username} size={150} round="1px" />
                )}
                {isLocalUser && (<button onClick={toggleVideo} style={{ backgroundColor: !videoEnabled ? '#4aed88' : ' #E4312b' }}>
                    {videoEnabled ? 'Disable Video' : 'Enable Video'}
                </button>
                )}
            </div>
            {remoteVideoRef.current && <video className='videoFeature' ref={remoteVideoRef} autoPlay playsInline />}
        </div>
    );
};

export default Video;
